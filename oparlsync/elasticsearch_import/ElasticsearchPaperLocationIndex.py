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

import json
from datetime import datetime
from ..models import *
from ..base_task import BaseTask
from mongoengine.base.datastructures import BaseList
from .ElasticsearchImportBase import ElasticsearchImportBase


class ElasticsearchPaperLocationIndex:

    def paper_location_index(self):
        if not self.es.indices.exists_alias(name='paper-location-latest'):
            now = datetime.utcnow()
            index_name = 'paper-location-' + now.strftime('%Y%m%d-%H%M')

            mapping = self.es_mapping_generator(Location, 'deref_paper_location')
            mapping['properties']['region'] = {
                'type': 'text'
            }
            mapping['properties']['legacy'] = {
                'type': 'boolean'
            }

            self.es.indices.create(index=index_name, body={
                'settings': self.es_settings(),
                'mappings': mapping
            })

            self.es.indices.update_aliases({
                'actions': {
                    'add': {
                        'index': index_name,
                        'alias': 'paper-location-latest'
                    }
                }
            })

        else:
            index_name = list(self.es.indices.get_alias('paper-location-latest'))[0]

        regions = []
        region = self.body.region
        while (region):
            regions.append(str(region.id))
            region = region.parent

        for location in Location.objects(body=self.body).no_cache():
            if location.deleted:
                self.es.delete(
                    index=index_name,
                    id=str(location.id),
                    ignore=[400, 404]
                )
                continue
            location_dict = location.to_dict(deref='deref_paper_location', format_datetime=True, delete='delete_paper_location', clean_none=True)
            location_dict['region'] = regions

            if 'geojson' in location_dict:
                if location_dict['geojson']:
                    if 'geometry' in location_dict['geojson']:
                        location_dict['geosearch'] = location_dict['geojson']['geometry']
                        if 'paper' in location_dict:
                            if type(location_dict['paper']) is list:
                                if 'properties' not in location_dict['geojson']:
                                    location_dict['geojson']['properties'] = {}
                                if not len(location_dict['paper']):
                                    continue
                                location_dict['geojson']['properties']['paper-count'] = len(location_dict['paper'])
                            else:
                                continue
                        else:
                            continue
                    else:
                        del location_dict['geojson']
                else:
                    del location_dict['geojson']
            if 'geojson' in location_dict:
                location_dict['geosearch'] = location_dict['geojson']['geometry']
                location_dict['geotype'] = location_dict['geojson']['geometry']['type']
                location_dict['geojson'] = json.dumps(location_dict['geojson'])

            location_dict['legacy'] = bool(location.region.legacy)
            try:
                new_doc = self.es.index(
                    index=index_name,
                    id=str(location.id),
                    body=location_dict
                )
                if new_doc['result'] in ['created', 'updated']:
                    self.statistics[new_doc['result']] += 1
                else:
                    self.datalog.warn('Unknown result at %s' % location.id)
            except BrokenPipeError:
                print('ignoring location %s because of size' % location.id)
                continue
        self.datalog.info('ElasticSearch paper-location import successfull: %s created, %s updated' % (
            self.statistics['created'],
            self.statistics['updated']
        ))
