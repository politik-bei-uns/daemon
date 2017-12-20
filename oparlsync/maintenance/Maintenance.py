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

    def remove(self, body_id):
        self.body_config = self.main.get_body_config(body_id)
        body = Body.objects(originalId=self.body_config['url']).first()
        if not body:
            sys.exit('body not found')
        # delete in mongodb

        for object in self.valid_objects:
            if object == Location:
                object.objects(bodies=body.id).delete()
            elif object != Body:
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
        for region_path in os.listdir(self.main.config.REGION_DIR):
            with open('%s/%s' % (self.main.config.REGION_DIR, region_path)) as region_file:
                region_data = json.load(region_file)
                if region_data['active']:
                    self.generate_region(region_data)

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
            Region.objects(rgs=region_data['rgs']).update_one(
                set__name=region_data['name'],
                set__level=region_data['osm_level'],
                set__rgs=region_data['rgs'],
                set__bounds=bounds,
                set__geojson=geojson,
                upsert=True,
            )

    def elasticsearch_regions(self):
        if not self.main.es.indices.exists_alias(name='region-latest'):
            now = datetime.utcnow()
            index_name = 'region-' + now.strftime('%Y%m%d-%H%M')

            es_import = ElasticsearchImport(self.main)
            self.main.es.indices.create(index=index_name, body={
                'settings': es_import.es_settings(),
                'mappings': {
                    'region': es_import.es_mapping_generator(Region)
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
                'coordinates': region_dict['bounds'],
            }
            region_dict['geojson'] = json.dumps(region_dict['geojson'])
            del region_dict['bounds']
            # try:
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
                location.locality = location.bodies[0].name
                if 'properties' not in location.geojson:
                    location.geojson['properties'] = {}
                location.geojson['properties']['locality'] = location.bodies[0].name