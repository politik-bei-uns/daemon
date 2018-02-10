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

import re
import os
import sys
import time
import json
import minio
import urllib
import hashlib
import datetime
import requests
import dateutil
from ssl import SSLError
from geojson import Feature
from urllib.parse import urlparse
from ..models import *
from pymongo import ReturnDocument
from bson.objectid import ObjectId
from minio.error import ResponseError, SignatureDoesNotMatch
from mongoengine.errors import ValidationError
from pymongo.errors import ServerSelectionTimeoutError


class OparlDownload():
    def __init__(self, main):
        self.start_time = datetime.datetime.utcnow()
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

        # set s3 files readonly if necessary
        if main.s3.get_bucket_policy(main.config.S3_BUCKET, "files") != 'readonly':
            main.s3.set_bucket_policy(main.config.S3_BUCKET, "files", minio.policy.Policy.READ_ONLY)

        # statistics
        self.mongodb_request_count = 0
        self.mongodb_request_cached = 0
        self.http_request_count = 0
        self.mongodb_request_time = 0
        self.file_download_time = 0
        self.download_not_required = 0
        self.http_request_time = 0
        self.minio_time = 0
        self.wait_time = 0

        self.main = main
        self.body_uid = False
        self.organization_list_url = False
        self.person_list_url = False
        self.meeting_list_url = False
        self.paper_list_url = False

        self.cache = {}
        for obj in self.valid_objects:
            self.cache[obj.__name__] = {}


    def __del__(self):
        pass

    def run(self, body_id, *args):
        if len(args):
            if args[0] == 'full':
                self.run_full(body_id, True)
            else:
                self.run_single(body_id, *args)
        else:
            self.run_full(body_id)

    def run_full(self, body_id, all=False):
        self.body_config = self.main.get_body_config(body_id)
        self.main.statuslog.info('Body %s sync launched.' % body_id)
        if not self.body_config:
            self.main.statuslog.error('body id %s configuration not found' % body_id)
            return
        if 'url' not in self.body_config:
            return
        start_time = time.time()
        self.get_body()
        if not self.body_uid:
            return
        for object in self.body_objects:
            self.get_list(object)

        # set last sync if everything is done so far
        body = Body.objects(id=self.body_uid).first()
        body.lastSync = self.start_time.isoformat()
        body.save()

        self.main.statuslog.info('Body %s sync done. Results:' % body_id)
        self.main.statuslog.info('mongodb requests:     %s' % self.mongodb_request_count)
        self.main.statuslog.info('cached requests:      %s' % self.mongodb_request_cached)
        self.main.statuslog.info('http requests:        %s' % self.http_request_count)
        self.main.statuslog.info('mongodb time:         %s s' % round(self.mongodb_request_time, 1))
        self.main.statuslog.info('minio time:           %s s' % round(self.minio_time, 1))
        self.main.statuslog.info('http time:            %s s' % round(self.http_request_time, 1))
        self.main.statuslog.info('file download time:   %s s' % round(self.file_download_time, 1))
        self.main.statuslog.info('download not reqired: %s' % self.download_not_required)
        self.main.statuslog.info('wait time:            %s s' % round(self.wait_time, 1))
        self.main.statuslog.info('app time:             %s s' % round(
            time.time() - start_time - self.mongodb_request_time - self.minio_time - self.http_request_time - self.wait_time - self.file_download_time,
            1))
        self.main.statuslog.info('all time:             %s s' % round(time.time() - start_time, 1))
        self.main.statuslog.info('processed %s objects per second' % round(self.mongodb_request_count / (time.time() - start_time), 1))

    def run_single(self, body_id, *args):
        self.body_config = self.main.get_body_config(body_id)
        self.last_update = False
        try:
            body = Body.objects(originalId=self.body_config['url']).no_cache().first()
        except ServerSelectionTimeoutError:
            sys.exit('fatal: MongoDB not available')
        if not body:
            sys.exit('fatal: body does not exist in database.')
        self.body_uid = body.id
        if len(args) != 1:
            sys.exit('fatal: to get a single dataset, please provide just one url or uid.')
        uid_pattern = re.compile("^([a-d0-9]){24}$")
        if uid_pattern.match(args[0]):
            self.run_single_by_uid(body_id, args[0])
            return
        try:
            url = urlparse(args[0])
        except ValueError:
            sys.exit('fatal: the argument is neither an url nor an uid.')
        if (url.scheme == 'http' or url.scheme == 'https') and url.netloc:
            self.run_single_by_url(body_id, args[0])
            return
        sys.exit('fatal: the argument is neither an url nor an id.')

    def run_single_by_uid(self, body_id, uid):
        pass

    def run_single_by_url(self, body_id, url):
        data = self.get_url_json(url)
        if not data:
            sys.exit('fatal: this is not an oparl object.')
        for oparl_object in self.valid_objects:
            if data['type'] == oparl_object.type:
                self.save_object(oparl_object, data)

    def get_body(self, set_last_sync=True):
        self.body_uid = False
        if self.main.config.USE_MIRROR:
            body_raw = self.get_url_json(self.main.config.OPARL_MIRROR_URL + '/body-by-id?id=' + urllib.parse.quote_plus(self.body_config['url']), wait=False)
        else:
            body_raw = self.get_url_json(self.body_config['url'], wait=False)
        if not body_raw:
            return
        # save originalId from body to ensure that uid can be used
        query = {
            'uid': self.body_config['id']
        }


        object_json = {
            '$set': {
                'rgs': self.body_config['rgs'],
                'uid': self.body_config['id']
            }
        }

        if 'legacy' not in self.body_config:
            object_json['$set']['originalId'] = body_raw[self.main.config.OPARL_MIRROR_PREFIX + ':originalId'] if self.main.config.USE_MIRROR else body_raw['id']
        if self.main.config.ENABLE_PROCESSING:
            region = Region.objects(rgs=self.body_config['rgs']).first()
            if region:
                object_json['$set']['region'] = region.id
        if self.main.config.USE_MIRROR:
            object_json['$set']['mirrorId'] = body_raw['id']
        self.correct_document_values(object_json['$set'])
        self.mongodb_request_count += 1
        start_time = time.time()
        result = self.main.db_raw.body.find_one_and_update(
            query,
            object_json,
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        self.body_uid = result['_id']

        if 'lastSync' in result:
            local_time_zone = dateutil.tz.gettz('Europe/Berlin')
            self.last_update = result['lastSync'].replace(microsecond=0, tzinfo=local_time_zone)
        else:
            self.last_update = False
        self.save_object(Body, body_raw)

        self.organization_list_url = body_raw['organization']
        self.person_list_url = body_raw['person']
        self.meeting_list_url = body_raw['meeting']
        self.paper_list_url = body_raw['paper']

        if self.main.config.USE_MIRROR and self.last_update:
            self.membership_list_url = body_raw['membership']
            self.agenda_item_list_url = body_raw['agendaItem']
            self.consultation_list_url = body_raw['consultation']
            self.location_list_url = body_raw['locationList']
            self.file_list_url = body_raw['file']
            self.body_objects += [
                Membership,
                AgendaItem,
                Consultation,
                File,
                Location
            ]

    def get_list(self, object, list_url=False):
        if list_url:
            object_list = self.get_url_json(list_url, is_list=True)
        else:
            if self.last_update and (self.body_config['force_full_sync'] == 0 or self.main.config.USE_MIRROR):
                last_update_tmp = self.last_update
                if self.main.config.USE_MIRROR:
                    last_update_tmp = last_update_tmp - datetime.timedelta(weeks=1)
                object_list = self.get_url_json(
                    getattr(self, '%s_list_url' % object._object_db_name) + '?modified_since=%s' % self.last_update.isoformat(),
                    is_list=True
                )
            else:
                object_list = self.get_url_json(getattr(self, '%s_list_url' % object._object_db_name), is_list=True)
        while object_list:
            for object_raw in object_list['data']:
                self.save_object(object, object_raw)
            if 'next' in object_list['links']:
                object_list = self.get_url_json(object_list['links']['next'], is_list=True)
            else:
                break

    def save_object(self, object, object_raw, validate=True):
        object_instance = object()
        dbref_data = {}

        # Stupid Bugfix for Person -> Location as locationObject
        if 'locationObject' in object_raw:
            object_raw['location'] = object_raw['locationObject']
            del object_raw['locationObject']

        # Iterate though all Objects and fix stuff (recursive)
        for key, value in object_raw.items():
            if key in object_instance._fields:
                # List of something
                if type(object_instance._fields[key]).__name__ == 'ListField':
                    external_list = False
                    if hasattr(object_instance._fields[key], 'external_list'):
                        if object_instance._fields[key].external_list:
                            continue
                    # List of relations
                    if type(object_instance._fields[key].field).__name__ == 'ReferenceField':
                        dbref_data[key] = []
                        for valid_object in self.valid_objects:
                            if valid_object.__name__ == object_instance._fields[key].field.document_type_obj:
                                for single in value:
                                    if valid_object.__name__ == 'Body':
                                        dbref_data[key].append(ObjectId(self.body_uid))
                                        continue
                                    if isinstance(single, dict) or key == 'derivativeFile':
                                        # we have to get derivativeFile now because it's in no other list
                                        if key == 'derivativeFile':
                                            sub_object_raw = self.get_url_json(single, False)
                                        else:
                                            sub_object_raw = single
                                        if 'created' not in sub_object_raw and 'created' in object_raw:
                                            sub_object_raw['created'] = object_raw['created']
                                        if 'modified' not in sub_object_raw and 'modified' in object_raw:
                                            sub_object_raw['modified'] = object_raw['modified']
                                        dbref_data[key].append(ObjectId(self.save_object(valid_object, sub_object_raw, True)['_id']))
                                    else:
                                        if single in self.cache[valid_object.__name__]:
                                            dbref_data[key].append(self.cache[valid_object.__name__][single])
                                            self.mongodb_request_cached += 1
                                            continue
                                        dbref_data[key].append(ObjectId(self.save_object(valid_object, {'id': single}, False)['_id']))
                    # List of Non-Relatipn
                    else:
                        self.save_document_values(object_instance, key, value)
                # Single Relation
                elif type(object._fields[key]).__name__ == 'ReferenceField':
                    for valid_object in self.valid_objects:
                        if valid_object.__name__ == object_instance._fields[key].document_type_obj:
                            if valid_object.__name__ == 'Body':
                                dbref_data[key] = ObjectId(self.body_uid)
                                continue
                            # Stupid bugfix for Person -> Location is an object id
                            if object.__name__ == 'Person' and valid_object.__name__ == 'Location' and isinstance(value, str) and not self.main.config.USE_MIRROR:
                                value = self.get_url_json(value)
                            if isinstance(value, dict):
                                sub_object_raw = value
                                if 'created' not in sub_object_raw and 'created' in object_raw:
                                    sub_object_raw['created'] = object_raw['created']
                                if 'modified' not in sub_object_raw and 'modified' in object_raw:
                                    sub_object_raw['modified'] = object_raw['modified']
                                dbref_data[key] = ObjectId(self.save_object(valid_object, sub_object_raw, True)['_id'])
                            else:
                                if value in self.cache[valid_object.__name__]:
                                    dbref_data[key] = self.cache[valid_object.__name__][value]
                                    self.mongodb_request_cached += 1
                                    continue
                                dbref_data[key] = ObjectId(self.save_object(valid_object, {'id': value}, False)['_id'])
                # No relation or list
                else:
                    self.save_document_values(object_instance, key, value)

        # Validate Object and log invalid objects
        if object != Body and validate:
            try:
                object_instance.validate()
            except ValidationError as err:
                self.main.datalog.warn(
                    '%s %s from Body %s failed validation.' % (object.__name__, object_raw['id'], self.body_uid))
        # fix modified
        if object_instance.created and object_instance.modified:
            if object_instance.created > object_instance.modified:
                object_instance.modified = object_instance.created

        # Etwas umständlicher Weg über pymongo
        if self.main.config.USE_MIRROR:
            query = {
                'mirrorId': object_instance.originalId
            }
            object_instance.mirrorId = object_instance.originalId
            if self.main.config.OPARL_MIRROR_PREFIX + ':originalId' in object_raw:
                object_instance.originalId = object_raw[self.main.config.OPARL_MIRROR_PREFIX + ':originalId']
            else:
                del object_instance.originalId
        else:
            query = {
                'originalId': object_instance.originalId
            }
        object_json = json.loads(object_instance.to_json())
        for field_key in object_json.keys():
            if type(object_instance._fields[field_key]).__name__ == 'DateTimeField':
                object_json[field_key] = getattr(object_instance, field_key)

        # Body ID related Fixes
        if object == Location:
            object_json['body'] = [self.body_uid]
            if 'geojson' in object_json:
                if 'geometry' in object_json['geojson']:
                    try:
                        geojson_check = Feature(geometry=object_json['geojson']['geometry'])
                        if not geojson_check.is_valid:
                            del object_json['geojson']
                            self.main.datalog.warn('invalid geojson found at %s' % object_instance.originalId)
                    except ValueError:
                        del object_json['geojson']
                        self.main.datalog.warn('invalid geojson found at %s' % object_instance.originalId)

        elif object == Body:
            if self.body_config['name']:
                object_json['name'] = self.body_config['name']
        else:
            object_json['body'] = self.body_uid

        # Set some File values if using mirror
        if object == File and self.main.config.USE_MIRROR:
            object_json['storedAtMirror'] = True
            if 'originalAccessUrl' in object_json:
                object_json['mirrorAccessUrl'] = object_json['originalAccessUrl']
            if 'originalDownloadUrl' in object_json:
                object_json['mirrorDownloadUrl'] = object_json['originalDownloadUrl']
            if self.main.config.OPARL_MIRROR_PREFIX + ':originalAccessUrl' in object_raw:
                object_json['originalAccessUrl'] = object_raw[self.main.config.OPARL_MIRROR_PREFIX + ':originalAccessUrl']
            if self.main.config.OPARL_MIRROR_PREFIX + ':originalDownloadUrl' in object_raw:
                object_json['originalDownloadUrl'] = object_raw[self.main.config.OPARL_MIRROR_PREFIX + ':originalDownloadUrl']

        # set all the dbrefs generated before
        object_json.update(dbref_data)

        # delete empty lists and dicts
        for key in list(object_json):
            if (isinstance(object_json[key], list) or isinstance(object_json[key], dict)) and not object_json[key]:
                del object_json[key]

        # Save data
        object_json = { '$set': object_json }
        self.correct_document_values(object_json['$set'])
        self.mongodb_request_count += 1
        start_time = time.time()
        result = self.main.db_raw[object._object_db_name].find_one_and_update(
            query,
            object_json,
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        self.main.datalog.debug('%s %s from Body %s saved successfully.' % (object.__name__, result['_id'], self.body_uid))
        self.mongodb_request_time += time.time() - start_time

        # Cache Original ID -> MongoDB ID
        if self.main.config.USE_MIRROR:
            if object_instance.mirrorId not in self.cache[object.__name__]:
                self.cache[object.__name__][object_instance.mirrorId] = ObjectId(result['_id'])
        else:
            if object_instance.originalId not in self.cache[object.__name__]:
                self.cache[object.__name__][object_instance.originalId] = ObjectId(result['_id'])

        # We need to download files if necessary
        if object == File and not self.main.config.USE_MIRROR:
            download_file = True
            if self.body_config['force_full_sync'] == 1 and self.last_update and object_instance.modified:
                if object_instance.modified < self.last_update:
                    download_file = False
            if 'downloaded' not in result:
                download_file = True
            elif not result['downloaded']:
                download_file = True
            if 'originalAccessUrl' in object_json['$set'] and download_file:
                file_name_internal = str(result['_id'])
                start_time = time.time()
                file_status = self.get_file(object_json['$set']['originalAccessUrl'], file_name_internal)
                self.file_download_time += time.time() - start_time
                if not file_status:
                    self.main.datalog.warn('No valid file could be downloaded at File %s from Body %s' % (result['_id'], self.body_uid))
                else:
                    start_time = time.time()
                    object_json_update = {}
                    mime_type = None
                    if 'mimeType' in object_json['$set']:
                        mime_type = object_json['$set']['mimeType']
                    file_name = None
                    if 'fileName' in object_json['$set']:
                        file_name = object_json['$set']['fileName']
                    else:
                        splitted_file_name = object_json['$set']['originalAccessUrl'].split('/')
                        if len(splitted_file_name):
                            if len(splitted_file_name[-1]) > 3 and '.' in splitted_file_name[-1]:
                                file_name = splitted_file_name[-1]
                    if not file_name or not mime_type:
                        self.main.datalog.warn('No file name or no mime type avaliable at File %s from Body %s' % (
                        result['_id'], self.body_uid))
                    else:
                        content_type = object_json['$set']['mimeType']
                        metadata = {
                            'Content-Disposition': 'filename=%s' % file_name
                        }
                        try:
                            self.main.s3.fput_object(
                                self.main.config.S3_BUCKET,
                                "files/%s/%s" % (self.body_uid, file_name_internal),
                                os.path.join(self.main.config.TMP_FILE_DIR, file_name_internal),
                                content_type=content_type,
                                metadata=metadata
                            )
                            self.main.datalog.debug('Binary file at File %s from Body %s saved successfully.' % (result['_id'], self.body_uid))
                            object_json_update['downloaded'] = True
                        except (ResponseError, SignatureDoesNotMatch) as err:
                            self.main.datalog.warn(
                                'Critical error saving file from File %s from Body %s' % (result['_id'], self.body_uid))
                    self.minio_time += time.time() - start_time
                    if 'size' not in object_json['$set']:
                        object_json_update['size'] = os.path.getsize(os.path.join(self.main.config.TMP_FILE_DIR, file_name_internal))
                    if 'sha1Checksum' not in object_json['$set'] or 'sha512Checksum' not in object_json['$set']:
                        with open(os.path.join(self.main.config.TMP_FILE_DIR, file_name_internal), 'rb') as checksum_file:
                            checksum_file_content = checksum_file.read()
                            if 'sha1Checksum' not in object_json['$set']:
                                object_json_update['sha1Checksum'] = hashlib.sha1(checksum_file_content).hexdigest()
                            if 'sha512Checksum' not in object_json['$set']:
                                object_json_update['sha512Checksum'] = hashlib.sha512(checksum_file_content).hexdigest()
                    if len(object_json_update.keys()):
                        result = self.main.db_raw[object._object_db_name].find_one_and_update(
                            query,
                            { '$set': object_json_update },
                            upsert=True,
                            return_document=ReturnDocument.AFTER
                        )
                        self.mongodb_request_count += 1
                    os.remove(os.path.join(self.main.config.TMP_FILE_DIR, file_name_internal))  # also get all derivativeFile
            else:
                self.download_not_required += 1

        # If we have a Paper with a Location, we need to mark this as official=ris relation
        if object == Paper and 'location' in object_json['$set']:
            for location_obj_id in object_json['$set']['location']:
                if not LocationOrigin.objects(paper=ObjectId(result['_id']), location=location_obj_id, origin='ris').no_cache().count():
                    location_origin = LocationOrigin()
                    location_origin.location = location_obj_id
                    location_origin.paper = ObjectId(result['_id'])
                    location_origin.origin = 'ris'
                    location_origin.save()
                    self.mongodb_request_count += 3
                    paper = Paper.objects(id=ObjectId(result['_id'])).no_cache().first()
                    if location_origin.id not in paper.locationOrigin:
                        paper.locationOrigin.append(location_origin.id)
                        paper.save()
                        self.mongodb_request_count += 1

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
            # temporary fix for missing body/1/
            if '/body/1' not in value:
                value = value.replace('/oparl/v1', '/oparl/v1/body/1')
            setattr(document, 'originalId', value)
        elif key == 'accessUrl':
            setattr(document, 'originalAccessUrl', value)
        elif key == 'downloadUrl':
            setattr(document, 'originalDownloadUrl', value)
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
            if r.status_code == 500:
                self.main.send_mail(
                    self.main.config.ADMINS,
                    'critical error at oparl-mirror',
                    'url %s throws an http error 500' % url
                )
                return False
            elif r.status_code == 200:
                try:
                    if not is_list:
                        return r.json()
                    else:
                        list_data = r.json()
                        if 'data' in list_data and 'links' in list_data:
                            return list_data
                except json.decoder.JSONDecodeError:
                    return False
        return False

    def get_file(self, url, file_name):
        try:
            r = requests.get(url, stream=True)
        except SSLError:
            return False
        if r.status_code != 200:
            return False
        with open(os.path.join(self.main.config.TMP_FILE_DIR, file_name), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        return True
