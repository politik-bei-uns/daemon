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
import json
import requests
import subprocess
from ..models import *
from geojson import Feature


class MaintenanceRegion:
    def generate_regions(self):
        max_level_overwrite = {}
        min_level_overwrite = {}
        for region_path in os.listdir(self.config.REGION_DIR):
            with open('%s/%s' % (self.config.REGION_DIR, region_path)) as region_file:
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
        for region_raw in Region.objects(parent__exists=False).order_by('name').all():
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
        with open(os.path.join(self.config.TMP_OSM_DIR, rgs + '.rel'), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        subprocess.call('perl %s < %s > %s' % (
            self.config.REL2POLY_PATH,
            os.path.join(self.config.TMP_OSM_DIR, rgs + '.rel'),
            os.path.join(self.config.TMP_OSM_DIR, rgs + '.poly')
        ), shell=True)
        geojson = {
            'type': 'Feature',
            'geometry': {
                'type': 'Polygon',
                'coordinates': [[]]
            }
        }
        with open(os.path.join(self.config.TMP_OSM_DIR, rgs + '.poly')) as poly_file:
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
                'set__legacy': 'legacy' in region_data,
                'upsert': True
            }
            if 'legacy' in region_data:
                if region_data['legacy']:
                    kwargs['legacy'] = True
            Region.objects(rgs=region_data['rgs']).update_one(**kwargs)

    def region_get_children(self, region_id):
        regions = []
        for region_raw in Region.objects(parent=region_id).order_by('name').all():
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