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
import mongoengine
from copy import deepcopy
from minio import Minio
from minio.policy import Policy
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, BucketAlreadyExists)
from urllib3.exceptions import MaxRetryError

from .config import get_config
from .mongoqueue import MongoQueue

from .oparl_download import OparlDownload
from .generate_thumbnails import GenerateThumbnails
from .generate_fulltext import GenerateFulltext
from .maintenance import Maintenance
from .street_import import StreetImport
from .generate_georeferences import GenerateGeoreferences
from .generate_backrefs import GenerateBackrefs
from .elasticsearch_import import ElasticsearchImport
from .misc import Misc


class Common():
    def __init__(self, prefix=''):
        self.prefix = prefix
        self.config = get_config(os.getenv('APPLICATION_MODE', 'DEVELOPMENT'))()
        self.init_logging()
        self.init_modules()
        self.init_db()
        self.init_queue()
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
        if self.config.ENABLE_PROCESSING:
            self.next_job = {
                'oparl_download': ['generate_backrefs', 'generate_thumbnails'],
                'generate_backrefs': ['generate_fulltext'],
                'generate_fulltext': ['generate_georeferences'],
                'generate_georeferences': ['elasticsearch_import'],
                'elasticsearch_import': ['misc']
            }
        else:
            self.next_job = {
                'oparl_download': ['generate_backrefs']
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
        self.datalog.addHandler(datalog_stream_handler)

        # full statuslog config
        self.statuslog = logging.getLogger('statuslog')
        self.statuslog.setLevel(logging.DEBUG)
        statuslog_file_handler = logging.FileHandler("%s/%s-status.log" % (self.config.LOG_DIR, self.prefix))
        statuslog_file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s ')
        )
        statuslog_file_handler.setLevel(logging.INFO)
        self.statuslog.addHandler(statuslog_file_handler)

        statuslog_stream_handler = logging.StreamHandler()
        statuslog_stream_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s ')
        )
        statuslog_stream_handler.setLevel(logging.DEBUG)
        self.statuslog.addHandler(statuslog_stream_handler)

    def update_datalog(self, module, body):
        for datalog_handler in self.datalog.handlers:
            self.datalog.removeHandler(datalog_handler)
        datalog_file_handler = logging.FileHandler(
            "%s/%s-%s-%s.log" % (self.config.LOG_DIR, (time.strftime("%Y-%m-%d--%H-%M-%S")), module, body))
        datalog_file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s ')
        )
        datalog_file_handler.setLevel(logging.INFO)
        self.datalog.addHandler(datalog_file_handler)

        datalog_stream_handler = logging.StreamHandler()
        datalog_stream_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s ')
        )
        datalog_stream_handler.setLevel(logging.DEBUG)
        self.datalog.addHandler(datalog_stream_handler)

    def init_modules(self):
        # Unterstützte Module
        self.modules = {
            'oparl_download': OparlDownload,
            'generate_thumbnails': GenerateThumbnails,
            'generate_fulltext': GenerateFulltext,
            'maintenance': Maintenance,
            'street_import': StreetImport,
            'generate_georeferences': GenerateGeoreferences,
            'generate_backrefs': GenerateBackrefs,
            'elasticsearch_import': ElasticsearchImport,
            'misc': Misc
        }

    def init_queue(self):
        # Queue (Netzwerk)
        self.queue_network = MongoQueue(
            self.db_raw.queue_network,
            consumer_id="main",
            timeout=300,
            max_attempts=3
        )

        # Queue (Local)
        self.queue_local = MongoQueue(
            self.db_raw.queue_local,
            consumer_id="main",
            timeout=300,
            max_attempts=3
        )

    def add_next_to_queue(self, current_module, body_id):
        if current_module in self.next_job.keys():
            for next_module in self.next_job[current_module]:
                payload = {
                    'module': next_module,
                    'body_id': body_id
                }
                self.queue_network.put(payload)

    def init_db(self):
        mongoengine.connect(
            db=self.config.MONGO_DB_NAME
        )
        try:
            self.db_raw = pymongo.MongoClient()
            self.db_raw.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as err:
            sys.exit('fatal: connection to MongoDB can\'t be established.')
        self.db_raw = self.db_raw[self.config.MONGO_DB_NAME]
        #
        self.s3 = Minio(self.config.S3_ENDPOINT,
                        access_key=self.config.S3_ACCESS_KEY,
                        secret_key=self.config.S3_SECRET_KEY,
                        secure=self.config.S3_SECURE)
        try:
            self.s3.make_bucket(self.config.S3_BUCKET,
                                location=self.config.S3_LOCATION)
        except BucketAlreadyOwnedByYou as err:
            pass
        except BucketAlreadyExists as err:
            pass
        except (MaxRetryError, ResponseError) as err:
            sys.exit('fatal: connection to Minio can\'t be established.')
        if self.s3.get_bucket_policy(self.config.S3_BUCKET, 'files') != 'readonly':
            self.s3.set_bucket_policy(self.config.S3_BUCKET, 'files', Policy.READ_ONLY)
        if self.s3.get_bucket_policy(self.config.S3_BUCKET, 'file-thumbnails') != 'readonly':
            self.s3.set_bucket_policy(self.config.S3_BUCKET, 'file-thumbnails', Policy.READ_ONLY)

        if self.config.ES_ENABLED:
            from elasticsearch import Elasticsearch
            self.es = Elasticsearch(
                self.config.ES_HOSTS
            )

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
        body_full = '\r\n'.join(['To: %s' % ', '.join(receivers),
                                 'From: %s' % self.config.MAIL_FROM,
                                 'Subject: %s' % subject,
                                 '', body]
                                )
        smtp.sendmail(
            self.config.MAIL_FROM,
            receivers,
            body_full
        )

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