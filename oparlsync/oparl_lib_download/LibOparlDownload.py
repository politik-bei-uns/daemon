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

import gi
import os
import time
import json
import datetime
import requests
import dateutil
from ..models import *
from pymongo import ReturnDocument
from bson.objectid import ObjectId

gi.require_version('OParl', '0.2')


class LibOparlDownload():

    def __init__(self, main):
        self.valid_objects = [
            Body,
            LegislativeTerm,
            Organization,
            Person,
            Membership,
            Meeting,
            AgendaItem,
            Consultation,
            Paper,
            File,
            Location
        ]
        self.body_objects = [
            Organization,
            Person,
            Meeting,
            Paper
        ]
        # statistics
        self.mongodb_request_count = 0
        self.http_request_count = 0
        self.mongodb_request_time = 0
        self.http_request_time = 0
        self.wait_time = 0

        self.main = main
        self.body_uid = False
        self.organization_list_url = False
        self.person_list_url = False
        self.meeting_list_url = False
        self.paper_list_url = False

    def __del__(self):
        pass

    def run(self, body_id):
        self.body_config = self.main.get_body_config(body_id)
        start_time = time.time()
        self.get_body()
        for object in self.body_objects:
            self.get_list(object)
        print('mongodb requests: %s' % self.mongodb_request_count)
        print('http requests: %s' % self.http_request_count)
        print('mongodb time: %s' % round(self.mongodb_request_time * 1000))
        print('http time: %s' % round(self.http_request_time * 1000))
        print('wait time: %s' % round(self.wait_time * 1000))
        print('all time: %s' % round((time.time() - start_time) * 1000))

    def resolve(_, url, status):
        try:
            req = urllib.request.urlopen(url)
            status = req.getcode()
            data = req.read()
            return data.decode('utf-8')
        except urllib.error.HTTPError as e:
            status = e.getcode()
            return None
        except Exception as e:
            status = -1
            return None

    def get_body(self):
        body =
        body_raw = self.get_url_json(self.body_config['url'], wait=False)
        if body_raw:
            result = self.save_object(Body, body_raw)

            self.body_uid = result['_id']
            self.organization_list_url = body_raw['organization']
            self.person_list_url = body_raw['person']
            self.meeting_list_url = body_raw['meeting']
            self.paper_list_url = body_raw['paper']

    def get_list(self, object):
        object_list = self.get_url_json(getattr(self, '%s_list_url' % object._object_db_name), is_list=True)
        while object_list:
            for object_raw in object_list['data']:
                self.save_object(object, object_raw)
            if 'next' in object_list['links']:
                object_list = self.get_url_json(object_list['links']['next'], is_list=True)
            else:
                object_list = False

    def save_object(self, object, object_raw):
        object_instance = object()
        dbref_data = {}
        for key, value in object_raw.items():
            if key in object_instance._fields:
                if type(object_instance._fields[key]).__name__ == 'ListField':
                    if type(object_instance._fields[key].field).__name__ == 'ReferenceField':
                        dbref_data[key] = []
                        for valid_object in self.valid_objects:
                            if valid_object.__name__ == object_instance._fields[key].field.document_type_obj:
                                for single in value:
                                    if isinstance(single, dict):
                                        sub_object_raw = single
                                    else:
                                        sub_object_raw = {
                                            'id': single
                                        }
                                    if valid_object._object_db_name == 'Body':
                                        dbref_data[key].append(ObjectId(self.body_uid))
                                    else:
                                        dbref_data[key].append(
                                            ObjectId(self.save_object(valid_object, sub_object_raw)['_id']))
                elif type(object._fields[key]).__name__ == 'ReferenceField':
                    for valid_object in self.valid_objects:
                        if valid_object.__name__ == object_instance._fields[key].document_type_obj:
                            if isinstance(value, dict):
                                sub_object_raw = value
                            else:
                                sub_object_raw = {
                                    'id': value
                                }
                            if valid_object.__name__ == 'Body':
                                dbref_data[key] = ObjectId(self.body_uid)
                            else:
                                dbref_data[key] = ObjectId(self.save_object(valid_object, sub_object_raw)['_id'])
                else:
                    self.save_document_values(object_instance, key, value)
        object_instance.validate()

        # Etwas umständlicher Weg über pymongo, aber upserts über MongoEngine sind erst recht PITA ("Django-style query keyword arguments" - argh)
        query = {
            'external_id': object_instance.external_id
        }
        object_json = json.loads(object_instance.to_json())
        if hasattr(object_instance, 'created'):
            object_json['created'] = object_instance.created
        if hasattr(object_instance, 'modified'):
            object_json['modified'] = object_instance.modified
        object_json.update(dbref_data)
        object_json = {
            '$set': object_json
        }
        self.correct_document_values(object_json['$set'])
        self.mongodb_request_count += 1
        start_time = time.time()
        # Blödes zusätzliches Query, um Änderungen in der sha1Checksum mitzubekommen - aber Frage: ist das überhaupt notwendig?
        if object == File:
            self.mongodb_request_count += 1
            file_status = self.main.db_raw[object._object_db_name].find_one(query)
        result = self.main.db_raw[object._object_db_name].find_one_and_update(
            query,
            object_json,
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        self.main.datalog.debug(
            '%s %s from Body %s saved successfully.' % (object.__name__, result['_id'], self.body_uid))
        self.mongodb_request_time += time.time() - start_time
        # Speichern der Dateien
        if object == File:
            if 'accessUrl' in object_json['$set']:
                download_file = True
                if file_status:
                    if 'sha1Checksum' in file_status and 'sha1Checksum' in result:
                        if file_status['sha1Checksum'] == result['sha1Checksum']:
                            download_file = False
                if download_file:
                    file_name = str(result['_id'])
                    if 'fileName' in object_json['$set']:
                        file_ending = object_json['$set']['fileName'].split('.')
                        if len(file_ending) >= 2:
                            file_name += '.' + file_ending[-1]
                    file_status = self.get_file(object_json['$set']['accessUrl'], file_name)
                    if not file_status:
                        self.main.datalog.warn('No valid file could be downloaded at File %s from Body %s' % (
                        result['_id'], self.body_uid))
                    try:
                        self.main.s3.fput_object(
                            self.main.config.S3_BUCKET,
                            "files/%s/%s" % (self.body_uid, file_name),
                            os.path.join(self.main.config.TMP_FILE_DIR, file_name)
                        )
                    except ResponseError as err:
                        self.main.datalog.warn(
                            'Critical error saving file from File %s from Body %s' % (result['_id'], self.body_uid))
                    self.main.datalog.debug(
                        'Binary file at File %s from Body %s saved successfully.' % (result['_id'], self.body_uid))
                    os.remove(os.path.join(self.main.config.TMP_FILE_DIR, file_name))
                else:
                    self.main.datalog.debug(
                        'Update of binary file at File %s from Body %s not necessary.' % (result['_id'], self.body_uid))
        return result

    ### global

    def save_document_values(self, document, key, value):
        if type(document._fields[key]).__name__ == 'DateTimeField':
            try:
                dt = dateutil.parser.parse(value)
            except ValueError:
                delattr(document, key)
                return
            if dt.tzname():
                setattr(document, key, dt.astimezone(timezone('UTC')).replace(tzinfo=None))
            else:
                setattr(document, key, dt)
        elif key == 'id':
            setattr(document, 'external_id', value)
        else:
            setattr(document, key, value)

    def correct_document_values(self, document_json):
        for key, value in document_json.items():
            if type(value) == type({}):
                if '$date' in value:
                    document_json[key] = datetime.datetime.fromtimestamp(value['$date'] / 1000).isoformat()

    def get_url_json(self, url, is_list=False, wait=True):
        if url:
            if wait:
                if 'wait_time' in self.body_config:
                    self.wait_time += self.body_config['wait_time']
                    time.sleep(self.body_config['wait_time'])
                else:
                    self.wait_time += self.main.config.GET_URL_WAIT_TIME
                    time.sleep(self.main.config.GET_URL_WAIT_TIME)
            self.main.datalog.info('%s: get %s' % (self.body_config['id'], url))
            self.http_request_count += 1
            start_time = time.time()
            r = requests.get(url)
            self.http_request_time += time.time() - start_time
            if r.status_code == 200:
                if not is_list:
                    return r.json()
                else:
                    list_data = r.json()
                    if 'data' in list_data and 'links' in list_data:
                        return list_data
        return False

    def get_file(self, url, file_name):
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            return False
        with open(os.path.join(self.main.config.TMP_FILE_DIR, file_name), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        return True
