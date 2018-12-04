# encoding: utf-8

"""
Copyright (c) 2017, Ernesto Ruge
All rights reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import os
import json
import requests
import subprocess
from geojson import Feature
from ..storage import Street, StreetNumber, Region
from .StreetCollector import StreetCollector
from ..base_task import BaseTask


class StreetImport(BaseTask):
    name = 'StreetImport'
    services = [
        'mongodb'
    ]

    def __init__(self, body_id):
        self.body_id = body_id
        super().__init__()

    def run(self, region_id):
        self.save_streets(region_id)

    def save_streets(self, region_id):
        if not self.config.ENABLE_PROCESSING:
            return
        self.region_config = self.get_region_config(region_id)
        if not self.region_config:
            return
        query = {
            'rgs': self.region_config['rgs']
        }
        object_json = {
            '$set': {
                'rgs': self.region_config['rgs']
            }
        }
        result = self.db_raw.region.find_one_and_update(
            query,
            object_json,
            upsert=True
        )
        self.region = Region.objects(rgs=self.region_config['rgs']).no_cache().first()
        self.region_uid = self.region.id

        if not self.region_config['osm_relation'] or not self.region_config['geofabrik_package']:
            self.datalog.error('fatal": missing osm_relation or geofabrik_package.')
            return

        # download and uncompress geofabrik data
        self.datalog.debug('downloading http://download.geofabrik.de/%s' % self.region_config['geofabrik_package'])
        r = requests.get('http://download.geofabrik.de/%s' % self.region_config['geofabrik_package'], stream=True)
        with open(os.path.join(self.config.TMP_OSM_DIR, region_id + '-geofabrik.osm.bz2'), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        self.datalog.debug('uncompressing %s-geofabrik.osm.bz2' % region_id)
        subprocess.call('bunzip2 %s' % (os.path.join(self.config.TMP_OSM_DIR, region_id + '-geofabrik.osm.bz2')),
                        shell=True)

        # download relation
        self.datalog.debug('downloading osm relation %s' % self.region_config['osm_relation'])
        r = requests.get('https://www.openstreetmap.org/api/0.6/relation/%s/full' % self.region_config['osm_relation'],
                         stream=True)
        with open(os.path.join(self.config.TMP_OSM_DIR, region_id + '.rel'), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)

        # transform relation to poly file
        self.datalog.debug('create poly file')
        subprocess.call('perl %s < %s > %s' % (
            self.config.REL2POLY_PATH,
            os.path.join(self.config.TMP_OSM_DIR, region_id + '.rel'),
            os.path.join(self.config.TMP_OSM_DIR, region_id + '.poly')
        ), shell=True)

        # process geofabrik data with relation
        self.datalog.debug('get all data inside of poly file')
        subprocess.call('%s --read-xml %s --bounding-polygon file=%s --write-xml file=%s' % (
            self.config.OSMOSIS_PATH,
            os.path.join(self.config.TMP_OSM_DIR, region_id + '-geofabrik.osm'),
            os.path.join(self.config.TMP_OSM_DIR, region_id + '.poly'),
            os.path.join(self.config.TMP_OSM_DIR, region_id + '.osm')
        ), shell=True)

        # collect streets and street numbers
        osm = StreetCollector()
        self.datalog.info("reading file ...")
        osm.apply_file(os.path.join(self.config.TMP_OSM_DIR, region_id + '.osm'))
        self.datalog.info("gathering streets ...")
        streets = {}
        for street_fragment in osm.street_fragments:
            if street_fragment['name'] not in streets:
                streets[street_fragment['name']] = {
                    'street': {
                        'type': 'Feature',
                        'geometry': {
                            'type': 'MultiLineString',
                            'coordinates': [],
                        },
                        'properties': {
                            'name': street_fragment['name'],
                        }
                    },
                    'numbers': {}
                }
            coordinates = []
            for point in street_fragment['nodes']:
                if point not in osm.nodes:
                    self.datalog.info("missing point %s" % point)
                    continue
                coordinates.append(osm.nodes[point])
            if len(coordinates) > 1:
                streets[street_fragment['name']]['street']['geometry']['coordinates'].append(coordinates)
            else:
                self.datalog.warn('multiline string fragment with len 1 found: %s' % json.dumps(coordinates))
        self.datalog.info("gathering addresses ...")
        for address in osm.addresses:
            if address['name'] not in streets:
                self.datalog.info("%s missing" % address['name'])
                continue
            base_address = {
                'type': 'Feature',
                'geometry': {
                },
                'properties': {
                    'name': address['name'],
                    'number': address['number']
                }
            }
            # set street name fields - and add them to street
            for field in ['postal_code', 'sub_locality', 'locality']:
                if field in address:
                    if address[field]:
                        base_address['properties'][field] = address[field]
                        if field not in streets[address['name']]['street']['properties']:
                            streets[address['name']]['street']['properties'][field] = []
                        if address[field] not in streets[address['name']]['street']['properties'][field]:
                            streets[address['name']]['street']['properties'][field].append(address[field])
            # set nodes
            if address['type'] == 'point':

                if point not in osm.nodes:
                    self.datalog.info("missing point %s" % point)
                    continue
                base_address['geometry'] = {
                    'type': 'Point',
                    'coordinates': osm.nodes[address['node']]
                }
            elif address['type'] == 'polygon':
                base_address['geometry'] = {
                    'type': 'Polygon',
                    'coordinates': [[]],
                }
                for point in address['nodes']:
                    if point not in osm.nodes:
                        self.datalog.info("missing point %s" % point)
                        continue
                    base_address['geometry']['coordinates'][0].append(osm.nodes[point])


            if address['number'] in streets[address['name']]['numbers'] and base_address['geometry']['type'] == 'Point':
                continue
            if address['number'] in streets[address['name']]['numbers'] and streets[address['name']]['numbers'][address['number']]['geometry']['type'] == 'Polygon' and base_address['geometry']['type'] != 'Point':
                streets[address['name']]['numbers'][address['number']]['geometry']['type'] = 'MultiPolygon'
                streets[address['name']]['numbers'][address['number']]['geometry']['coordinates'] = [streets[address['name']]['numbers'][address['number']]['geometry']['coordinates']]
                streets[address['name']]['numbers'][address['number']]['geometry']['coordinates'].append(base_address['geometry']['coordinates'])
            else:
                streets[address['name']]['numbers'][address['number']] = base_address

        for street_name, street in streets.items():
            street_obj = Street.objects(region=self.region, streetName = street['street']['properties']['name']).first()
            if not street_obj:
                street_obj = Street()
                street_obj.region = self.region
                street_obj.streetName = street['street']['properties']['name']
            if 'postal_code' in street['street']['properties']:
                street_obj.postalCode = street['street']['properties']['postal_code']
            if 'sub_locality' in street['street']['properties']:
                street_obj.subLocality = street['street']['properties']['sub_locality']
            if 'locality' in street['street']['properties']:
                street_obj.locality = street['street']['properties']['locality']
            else:
                street_obj.locality = [self.region.name]
                street['street']['properties']['locality'] = [self.region.name]

            # validate geojson
            geojson_check = Feature(geometry=street['street']['geometry'])
            if geojson_check.is_valid and street['street']['geometry']['coordinates']:
                street_obj.geojson = street['street']
            else:
                self.datalog.warn('invalid location found: %s' % json.dumps(street['street']['geometry']))
            for street_number_name, street_number in street['numbers'].items():
                street_number_obj = StreetNumber.objects(region=self.region, streetName = street_number['properties']['name'], streetNumber=street_number['properties']['number']).first()
                if not street_number_obj:
                    street_number_obj = StreetNumber()
                    street_number_obj.region = self.region
                    street_number_obj.streetName = street_number['properties']['name']
                    street_number_obj.streetNumber = street_number['properties']['number']
                if 'postal_code' in street_number:
                    street_number_obj.postalCode = street_number['properties']['postal_code']
                if 'sub_locality' in street_number:
                    street_number_obj.subLocality = street_number['properties']['sub_locality']
                if 'locality' in street_number:
                    street_number_obj.locality = street_number['properties']['locality']
                else:
                    street_number_obj.locality = self.region.name
                    street_number['properties']['locality'] = self.region.name
                # validate geojson
                geojson_check = Feature(geometry=street_number['geometry'])
                if geojson_check.is_valid and street_number['street']['geometry']['coordinates']:
                    street_number_obj.geojson = street_number
                else:
                    self.datalog.warn('invalid location found: %s' % json.dumps(street_number['geometry']))
                street_number_obj.save()
                if street_number_obj not in street_obj.streetNumber:
                    street_obj.streetNumber.append(street_number_obj)
            street_obj.save()
        self.datalog.debug('tidy up')

        os.remove(os.path.join(self.config.TMP_OSM_DIR, region_id + '.rel'))
        os.remove(os.path.join(self.config.TMP_OSM_DIR, region_id + '.osm'))
        os.remove(os.path.join(self.config.TMP_OSM_DIR, region_id + '.poly'))
        os.remove(os.path.join(self.config.TMP_OSM_DIR, region_id + '-geofabrik.osm'))

