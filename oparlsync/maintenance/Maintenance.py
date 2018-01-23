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
import hashlib
import threading
from pymongo import MongoClient
from _datetime import datetime
from geojson import Feature
import requests
import subprocess
import elasticsearch
from ..models import *
from ..elasticsearch_import import ElasticsearchImport
from minio.error import ResponseError


class Maintenance():
    def __init__(self, main):
        self.main = main
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

    def run(self, body_id, *args):
        if len(args) < 1:
            sys.exit('usage: python manage.py maintenance $body_id remove|clean')

        if args[0] == 'remove':
            self.remove(body_id)
        elif args[0] == 'clean':
            self.clean(body_id)
        elif args[0] == 'generate_regions':
            self.generate_regions()
        elif args[0] == 'elasticsearch_regions':
            self.elasticsearch_regions()
        elif args[0] == 'update_street_locality':
            self.update_street_locality()
        elif args[0] == 'old_import':
            self.old_import()
        elif args[0] == 'sync_bodies':
            self.sync_bodies()
        elif args[0] == 'sync_body':
            self.sync_body(body_id)
        elif args[0] == 'correct_regions':
            self.correct_regions()
        elif args[0] == 'delete_all_locations':
            self.delete_all_locations(body_id)
        elif args[0] == 'delete_last_sync':
            self.delete_last_sync(body_id)

    def remove(self, body_id):
        self.body_config = self.main.get_body_config(body_id)
        body = Body.objects(originalId=self.body_config['url']).first()
        if not body:
            sys.exit('body not found')
        # delete in mongodb

        for object in self.valid_objects:
            if object != Body:
                object.objects(body=body.id).delete()
        # delete in minio
        try:
            get_name = lambda object: object.object_name
            names = map(get_name, self.main.s3.list_objects_v2(self.main.config.S3_BUCKET, 'files/%s' % str(body.id),
                                                               recursive=True))
            for error in self.main.s3.remove_objects(self.main.config.S3_BUCKET, names):
                self.main.datalog.warn(
                    'Critical error deleting file from File %s from Body %s' % (error.object_name, body.id))
        except ResponseError as err:
            self.main.datalog.warn('Critical error deleting files from Body %s' % body.id)

        body.delete()

    def clean(self, body_id):
        pass

    def generate_regions(self):

        max_level_overwrite = {}
        min_level_overwrite = {}
        for region_path in os.listdir(self.main.config.REGION_DIR):
            with open('%s/%s' % (self.main.config.REGION_DIR, region_path)) as region_file:
                region_data = json.load(region_file)
                if region_data['active']:# and 'legacy' not in region_data:
                    self.generate_region(region_data)
                    if 'osm_level_max' in region_data:
                        max_level_overwrite[region_data['rgs']] = region_data['osm_level_max']
                    if 'osm_level_min' in region_data:
                        min_level_overwrite[region_data['rgs']] = region_data['osm_level_min']

        for parent_region in Region.objects.order_by('level').all():
            next_level = 10
            for child_region in Region.objects(rgs__startswith = parent_region.rgs, level__gt = parent_region.level).all():
                if child_region.level < next_level:
                    next_level = child_region.level
            if next_level < 10:
                for child_region in Region.objects(rgs__startswith=parent_region.rgs, level=next_level).all():
                    child_region.parent = parent_region.id
                    child_region.save()

            if parent_region.parent:
                parent_region.level_min = parent_region.parent.level_max
            else:
                parent_region.level_min = parent_region.level
            if parent_region.rgs in max_level_overwrite:
                parent_region.level_max = max_level_overwrite[parent_region.rgs]
            else:
                parent_region.level_max = next_level

            rgs = parent_region.rgs

            parent_region.body = []
            for body in Body.objects(rgs=rgs).all():
                parent_region.body.append(body.id)
            parent_region.save()

        regions = []
        for region_raw in Region.objects(parent__exists=False).all():
            regions.append({
                'id': str(region_raw.id),
                'name': region_raw.name,
                'rgs': region_raw.rgs,
                'level': region_raw.level,
                'children': self.region_get_children(region_raw.id)
            })
        option = Option.objects(key='region_cache').first()
        if not option:
            option = Option()
            option.key = 'region_cache'
        option.value = json.dumps(regions)
        option.save()



    def generate_region(self, region_data):
        rgs = region_data['rgs']
        r = requests.get('https://www.openstreetmap.org/api/0.6/relation/%s/full' % region_data['osm_relation'], stream=True)
        with open(os.path.join(self.main.config.TMP_OSM_DIR, rgs + '.rel'), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        subprocess.call('perl %s < %s > %s' % (
            self.main.config.REL2POLY_PATH,
            os.path.join(self.main.config.TMP_OSM_DIR, rgs + '.rel'),
            os.path.join(self.main.config.TMP_OSM_DIR, rgs + '.poly')
        ), shell=True)
        geojson = {
            'type': 'Feature',
            'geometry': {
                'type': 'Polygon',
                'coordinates': [[]]
            }
        }
        with open(os.path.join(self.main.config.TMP_OSM_DIR, rgs + '.poly')) as poly_file:
            lines = poly_file.readlines()
            first = True
            for item in lines:
                data = item.split()
                if len(data) == 2:
                    if first:
                        first = False
                        bounds = [
                            [float(data[0]), float(data[1])],
                            [float(data[0]), float(data[1])]
                        ]
                    if bounds[0][0] > float(data[0]):
                        bounds[0][0] = float(data[0])
                    if bounds[0][1] < float(data[1]):
                        bounds[0][1] = float(data[1])
                    if bounds[1][0] < float(data[0]):
                        bounds[1][0] = float(data[0])
                    if bounds[1][1] > float(data[1]):
                        bounds[1][1] = float(data[1])
                    geojson['geometry']['coordinates'][0].append([float(data[0]), float(data[1])])
        geojson_check = Feature(geometry=geojson['geometry'])
        geojson['properties'] = {
            'name': region_data['name'],
            'level': region_data['osm_level'],
            'rgs': region_data['rgs']
        }
        if geojson_check.is_valid:
            kwargs = {
                'set__name': region_data['name'],
                'set__level': region_data['osm_level'],
                'set__rgs': region_data['rgs'],
                'set__bounds': bounds,
                'set__geojson': geojson,
                'upsert': True
            }
            if 'legacy' in region_data:
                if region_data['legacy']:
                    kwargs['legacy'] = True
            Region.objects(rgs=region_data['rgs']).update_one(**kwargs)



    def region_get_children(self, region_id):
        regions = []
        for region_raw in Region.objects(parent=region_id).all():
            region = {
                'id': str(region_raw.id),
                'name': region_raw.name,
                'rgs': region_raw.rgs,
                'level': region_raw.level,
                'body': []
            }
            for body in region_raw.body:
                region['body'].append(str(body.id))
            children = self.region_get_children(region_raw.id)
            if len(children):
                region['children'] = children
            regions.append(region)
        return regions

    def elasticsearch_regions(self):
        if not self.main.es.indices.exists_alias(name='region-latest'):
            now = datetime.utcnow()
            index_name = 'region-' + now.strftime('%Y%m%d-%H%M')

            es_import = ElasticsearchImport(self.main)

            mapping = es_import.es_mapping_generator(Region, deref='deref_region', delete='delete_region')
            mapping['properties']['body_count'] = {
                'type': 'integer'
            }
            self.main.es.indices.create(index=index_name, body={
                'settings': es_import.es_settings(),
                'mappings': {
                    'region': mapping
                }
            })

            self.main.es.indices.update_aliases({
                'actions': {
                    'add': {
                        'index': index_name,
                        'alias': 'region-latest'
                    }
                }
            })

        else:
            index_name = list(self.main.es.indices.get_alias('region-latest'))[0]

        for region in Region.objects():
            region_dict = region.to_dict()
            region_dict['geosearch'] = {
                'type': 'envelope',
                'coordinates': region_dict['bounds']
            }
            region_dict['geojson']['properties']['legacy'] = region.legacy
            region_dict['geojson']['properties']['bodies'] = []
            region_dict['body_count'] = len(region.body)
            for body in region.body:
                region_dict['geojson']['properties']['bodies'].append(str(body.id))

            region_dict['geojson'] = json.dumps(region_dict['geojson'])
            del region_dict['bounds']



            new_doc = self.main.es.index(
                index=index_name,
                id=str(region.id),
                doc_type='region',
                body=region_dict
            )

    def update_street_locality(self):
        for street in Street.objects():
            if not len(street.locality):
                street.locality = [street.body.name]
                street.geojson['properties']['locality'] = [street.body.name]
                street.save()
                print('modified %s' % street.streetName)
        for street in StreetNumber.objects():
            if not street.locality:
                street.locality = street.body.name
                if 'properties' not in street.geojson:
                    street.geojson['properties'] = {}
                street.geojson['properties']['locality'] = street.body.name
                street.save()
                print('modified %s' % street.streetName)
        for location in Location.objects():
            if not location.locality:
                location.locality = location.body[0].name
                if 'properties' not in location.geojson:
                    location.geojson['properties'] = {}
                location.geojson['properties']['locality'] = location.body[0].name

    def sync_bodies(self):
        for body in Body.objects.all():
            self.sync_body(body.uid)

    def sync_body(self, body_id):
        self.body_config = self.main.get_body_config(body_id)
        query = {
            'uid': body_id
        }
        object_json = {
            '$set': {
                'uid': body_id,
                'rgs': self.body_config['rgs']
            }
        }
        if self.main.config.ENABLE_PROCESSING:
            region = Region.objects(rgs=self.body_config['rgs']).first()
            if region:
                object_json['$set']['region'] = region.id
        self.main.db_raw.body.find_one_and_update(
            query,
            object_json,
            upsert=True
        )

    def old_import(self):
        client = MongoClient()
        db = client.ris
        for body_raw in db.body.find():
            body = Body()
            body.id = body_raw['_id']
            print('save body %s' % body.id)
            body.legacy = True
            body.rgs = body_raw['regionalschlüssel']
            body.name = body_raw['name']
            body.created = body_raw['created']
            body.modified = body_raw['modified']
            body.save()


        for paper_raw in db.paper.find(no_cursor_timeout=True):
            if 'body' not in paper_raw:
                continue
            paper = Paper()
            paper.id = paper_raw['_id']
            paper.legacy = True
            print('save paper %s' % paper.id)

            paper.body = paper_raw['body'].id

            if 'name' in paper_raw:
                paper.name = paper_raw['name']
            elif 'title' in paper_raw:
                paper.name = paper_raw['title']

            if 'reference' in paper_raw:
                paper.reference = paper_raw['reference']
            elif 'nameShort' in paper_raw:
                paper.reference = paper_raw['nameShort']

            if 'publishedDate' in paper_raw:
                paper.date = paper_raw['publishedDate']

            if 'paperType' in paper_raw:
                paper.paperType = paper_raw['paperType']
            if 'created' in paper_raw:
                paper.created = paper_raw['created']
            if 'modified' in paper_raw:
                paper.modified = paper_raw['modified']

            if 'mainFile' in paper_raw:
                file = File()
                file.id = paper_raw['mainFile'].id
                file.save()
                paper.mainFile = file.id
            if 'auxiliaryFile' in paper_raw:
                paper.auxiliaryFile = []
                for file_raw in paper_raw['auxiliaryFile']:
                    file = File()
                    file.id = file_raw.id
                    file.save()
                    paper.auxiliaryFile.append(file.id)

            paper.save()


        for file_raw in db.file.find(no_cursor_timeout=True):
            if 'body' not in file_raw or 'mimetype' not in file_raw:
                continue
            file = File()
            file.id = file_raw['_id']
            file.legacy = True
            file.mimeType = file_raw['mimetype']
            file.body = file_raw['body'].id
            print('save file %s from body %s' % (file.id, file.body))
            if 'filename' in file_raw:
                file.fileName = file_raw['filename']
            if 'name' in file_raw:
                file.fileName = file_raw['name']
            if 'created' in file_raw:
                file.created = file_raw['created']
            if 'modified' in file_raw:
                file.modified = file_raw['modified']
            if 'filename' in file_raw:
                file.fileName = file_raw['filename']

            r = requests.get('https://politik-bei-uns.de/file/%s/download' % file.id, stream=True)

            if r.status_code != 200:
                file.downloaded = False
            else:
                file_path = os.path.join(self.main.config.TMP_OLD_IMPORT_DIR, str(file.id))
                with open(file_path, 'wb') as file_data:
                    for chunk in r.iter_content(chunk_size=32 * 1024):
                        if chunk:
                            file_data.write(chunk)
                file.size = os.path.getsize(file_path)
                with open(file_path, 'rb') as checksum_file:
                    checksum_file_content = checksum_file.read()
                    file.sha1Checksum = hashlib.sha1(checksum_file_content).hexdigest()

                metadata = {
                    'Content-Disposition': 'filename=%s' % file.name
                }

                self.main.s3.fput_object(
                    self.main.config.S3_BUCKET,
                    "files/%s/%s" % (file.body, file.id),
                    file_path,
                    content_type=file.mimeType,
                    metadata= {
                        'Content-Disposition': 'filename=%s' % file.name
                    }
                )

                file.downloaded = True
            file.save()
            print('thread count: %s' % threading.active_count())

    def correct_regions(self):
        for body in Body.objects.no_cache().all():
            if body.region:
                for street in Street.objects(body=body).no_cache().all():
                    street.region = body.region
                    street.save()
                for street_number in StreetNumber.objects(body=body).no_cache().all():
                    street_number.region = body.region
                    street_number.save()

    def delete_all_locations(self, body):
        Location.objects.delete()

        query = {}
        if body != 'all':
            query['uid'] = body
        object_json = {
            '$unset': {
                'location': ''
            }
        }
        self.main.db_raw.body.update_many(
            query,
            object_json
        )
        self.main.db_raw.person.update_many(
            query,
            object_json
        )
        self.main.db_raw.organization.update_many(
            query,
            object_json
        )
        self.main.db_raw.meeting.update_many(
            query,
            object_json
        )
        self.main.db_raw.paper.update_many(
            query,
            object_json
        )
        self.main.db_raw.street.update_many(
            query,
            object_json
        )
        self.main.db_raw.street_number.update_many(
            query,
            object_json
        )

    def delete_last_sync(self, body):
        query = {}
        if body != 'all':
            query['uid'] = body

        object_json = {
            '$unset': {
                'lastSync': ''
            }
        }
        self.main.db_raw.body.update_many(
            query,
            object_json
        )