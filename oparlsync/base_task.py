# encoding: utf-8

"""
Copyright (c) 2012 - 2016, Ernesto Ruge
All rights reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import os
import sys
import json
import time
import logging
import pymongo
import smtplib
import requests
import mongoengine
import subprocess
from copy import deepcopy
from minio import Minio
from minio.error import NoSuchBucketPolicy
from elasticsearch import Elasticsearch
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, BucketAlreadyExists, NoSuchKey)
from urllib3.exceptions import MaxRetryError, ClosedPoolError
from mongoengine.connection import disconnect as mongoengine_disconnect

from .config import get_config



class BaseTask():
    name = 'BaseTask'
    body_id = None
    services = []

    def __init__(self):
        self.config = get_config(os.getenv('APPLICATION_MODE', 'DEVELOPMENT'))()
        self.init_logging()
        self.init_db()
        self.default_config = {
            "id": "",
            "rgs": "",
            "url": "",
            "force_full_sync": 0,
            "wait_time": 0.2,
            "geofabrik_package": False,
            "osm_relation": False,
            "name": False
        }


    def init_logging(self):
        # prepare datalog
        self.datalog = logging.getLogger('datalog')
        self.datalog.setLevel(logging.DEBUG)
        datalog_stream_handler = logging.StreamHandler()
        datalog_stream_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s ')
        )
        datalog_stream_handler.setLevel(logging.DEBUG)

        datalog_file_handler = logging.FileHandler("%s/%s-%s-%s.log" % (self.config.LOG_DIR, (time.strftime("%Y-%m-%d--%H-%M-%S")), self.name, self.body_id))
        datalog_file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s ')
        )
        datalog_file_handler.setLevel(logging.DEBUG)
        self.datalog.addHandler(datalog_file_handler)

        datalog_stream_handler = logging.StreamHandler()
        datalog_stream_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s ')
        )
        datalog_stream_handler.setLevel(logging.DEBUG)
        self.datalog.addHandler(datalog_stream_handler)

    def init_db(self):
        if 'mongodb' in self.services:
            mongoengine.connect(
                db=self.config.MONGO_DB_NAME,
                host=self.config.MONGO_DB_HOST,
                port=self.config.MONGO_DB_PORT,
                connect=False
            )
            try:
                self.db_raw_client = pymongo.MongoClient(
                    host=self.config.MONGO_DB_HOST,
                    port=self.config.MONGO_DB_PORT,
                    connect=False
                )
                self.db_raw_client.server_info()
            except pymongo.errors.ServerSelectionTimeoutError as err:
                sys.exit('fatal: connection to MongoDB can\'t be established.')
            self.db_raw = self.db_raw_client[self.config.MONGO_DB_NAME]
        self.s3 = None
        if 's3' in self.services:
            self.s3 = Minio(self.config.S3_ENDPOINT,
                            access_key=self.config.S3_ACCESS_KEY,
                            secret_key=self.config.S3_SECRET_KEY,
                            secure=self.config.S3_SECURE)
            try:
                if self.s3.bucket_exists(self.config.S3_BUCKET):
                    self.s3.make_bucket(self.config.S3_BUCKET, location=self.config.S3_LOCATION)
            except (MaxRetryError, ResponseError) as err:
                sys.exit('fatal: connection to Minio can\'t be established.')
            # Policies
            needs_policy_update = False
            try:
                policies = json.loads(self.s3.get_bucket_policy(self.config.S3_BUCKET).decode("utf-8") )
                if self.config.ENABLE_PROCESSING and len(policies['Statement']) == 1:
                    needs_policy_update = True
            except NoSuchBucketPolicy:
                needs_policy_update = True
            if needs_policy_update:
                policies =  [
                    {
                        'Sid': '',
                        'Effect': 'Allow',
                        'Principal': {'AWS': '*'},
                        'Action': 's3:GetObject',
                        'Resource': 'arn:aws:s3:::%s/files/*' % self.config.S3_BUCKET
                    }
                ]
                if self.config.ENABLE_PROCESSING:
                    policies.append(
                        {
                            'Sid': '',
                            'Effect': 'Allow',
                            'Principal': {'AWS': '*'},
                            'Action': 's3:GetObject',
                            'Resource': 'arn:aws:s3:::%s/file-thumbnails/*' % self.config.S3_BUCKET
                        }
                    )
                self.s3.set_bucket_policy(
                    self.config.S3_BUCKET,
                    json.dumps({
                        'Version': '2018-01-01',
                        'Statement': policies
                    })
                )
        self.es = None
        if 'elasticsearch' in self.services:
            self.es = Elasticsearch(
                self.config.ES_HOSTS
            )
    def close(self):
        self.close_connections()
        self.close_logging()

    def close_connections(self):
        self.db_raw_client.close()
        self.db_raw_client = None
        mongoengine_disconnect()
        if self.config.ES_ENABLED and self.es:
            for conn in self.es.transport.connection_pool.connections:
                conn.pool.close()
            self.es = None

    def close_logging(self):
        for handler in self.datalog.handlers:
            self.datalog.removeHandler(handler)

    def get_file(self, file, save_to):
        if file.storedAtMirror:
            if not file.mirrorAccessUrl:
                return False
            r = requests.get(file.mirrorAccessUrl, stream=True)
            if r.status_code != 200:
                return False
            with open(save_to, 'wb') as file_data:
                for chunk in r.iter_content(chunk_size=32 * 1024):
                    if chunk:
                        file_data.write(chunk)
            return True
        else:
            try:
                data = self.s3.get_object(
                    self.config.S3_BUCKET,
                    "files/%s/%s" % (file.body.id, file.id)
                )
            except NoSuchKey:
                return False
            with open(save_to, 'wb') as file_data:
                for chunk in data.stream(32 * 1024):
                    file_data.write(chunk)
            return True

    def execute(self, cmd, body_id):
        new_env = os.environ.copy()
        new_env['XDG_RUNTIME_DIR'] = '/tmp/'
        try:
            output, error = subprocess.Popen(
                cmd.split(' '),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=new_env
            ).communicate(timeout=self.config.SUBPROCESS_TIMEOUT)
        except subprocess.TimeoutExpired:
            self.send_mail(
                self.config.ADMINS,
                'critical error at oparl-mirror',
                'command %s at %s takes forever' % (cmd, body_id)
            )
            return False
        try:
            if error is not None and error.decode().strip() != '' and 'WARNING **: clutter failed 0, get a life.' not in error.decode():
                self.datalog.debug("pdf output at command %s; output: %s" % (cmd, error.decode()))
        except UnicodeDecodeError:
            self.datalog.debug("pdf output at command %s; output: %s" % (cmd, error))
        return output

    def get_body_config(self, body_id=False, filename=False):
        if not filename:
            filename = '%s.json' % (body_id)
        try:
            with open('%s/%s' % (self.config.BODY_DIR, filename)) as body_config_file:
                if not body_config_file:
                    return False
                try:
                    body_config = deepcopy(self.default_config)
                    body_config.update(json.load(body_config_file))
                    return body_config
                except ValueError:
                    return False
        except FileNotFoundError:
            return False

    def get_region_config(self, rgs):
        try:
            with open('%s/%s.json' % (self.config.REGION_DIR, rgs)) as region_config_file:
                if not region_config_file:
                    return False
                try:
                    return json.load(region_config_file)
                except ValueError:
                    return False
        except FileNotFoundError:
            return False

    def send_mail(self, receivers=None, subject='', body=''):
        smtp = smtplib.SMTP(
            host=self.config.MAIL_HOST
        )
        smtp.ehlo_or_helo_if_needed()
        if self.config.MAIL_USE_SSL:
            smtp.starttls()
            smtp.ehlo_or_helo_if_needed()
        smtp.login(
            user=self.config.MAIL_USERNAME,
            password=self.config.MAIL_PASSWORD
        )
        body_full = '\r\n'.join([
            'To: %s' % ', '.join(receivers),
            'From: %s' % self.config.MAIL_FROM,
            'Subject: %s' % subject,
            '', body
        ])
        smtp.sendmail(
            self.config.MAIL_FROM,
            receivers,
            body_full
        )