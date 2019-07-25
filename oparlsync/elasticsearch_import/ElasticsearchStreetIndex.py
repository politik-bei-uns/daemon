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


class ElasticsearchStreetIndex:

    def street_index(self):
        if not self.es.indices.exists_alias(name='street-latest'):
            now = datetime.utcnow()
            index_name = 'street-' + now.strftime('%Y%m%d-%H%M')

            mapping = self.es_mapping_generator(Street, 'deref_street')

            mapping['properties']['autocomplete'] = {
                "type": 'text',
                "analyzer": "autocomplete_import_analyzer",
                "search_analyzer": "autocomplete_search_analyzer"
            }
            mapping['properties']['legacy'] = {
                'type': 'boolean'
            }

            self.es.indices.create(index=index_name, body={
                'settings': self.es_settings(),
                'mappings':  mapping
            })

            self.es.indices.update_aliases({
                'actions': {
                    'add': {
                        'index': index_name,
                        'alias': 'street-latest'
                    }
                }
            })
        else:
            index_name = list(self.es.indices.get_alias('street-latest'))[0]

        for street in Street.objects(region=self.body.region).no_cache():
            street_dict = street.to_dict(deref='deref_street', format_datetime=True, delete='delete_street', clean_none=True)

            if 'geojson' in street_dict:
                if street_dict['geojson']:
                    if 'geometry' in street_dict['geojson']:
                        street_dict['geosearch'] = street_dict['geojson']['geometry']
                    else:
                        del street_dict['geojson']
                else:
                    del street_dict['geojson']
            if 'geojson' in street_dict:
                street_dict['geosearch'] = street_dict['geojson']['geometry']
                street_dict['geotype'] = street_dict['geojson']['geometry']['type']
                street_dict['geojson'] = json.dumps(street_dict['geojson'])

            street_dict['autocomplete'] = ''
            if 'streetName' in street_dict:
                if street_dict['streetName']:
                    street_dict['autocomplete'] = street_dict['streetName'] + ', '

            if 'postalCode' in street_dict:
                if street_dict['postalCode']:
                    street_dict['autocomplete'] += street_dict['postalCode'][0] + ' '

            if 'locality' in street_dict:
                if street_dict['locality']:
                    street_dict['autocomplete'] += street_dict['locality'][0]

            if 'subLocality' in street_dict:
                if street_dict['subLocality']:
                    street_dict['autocomplete'] += ' (' + street_dict['subLocality'][0] + ')'

            street_dict['legacy'] = bool(street.region.legacy)
            try:
                new_doc = self.es.index(
                    index=index_name,
                    id=str(street.id),
                    body=street_dict
                )
            except:
                continue

            if new_doc['result'] in ['created', 'updated']:
                self.statistics[new_doc['result']] += 1
            else:
                self.datalog.warn('Unknown result at %s' % street.id)
        self.datalog.info('ElasticSearch street import successfull: %s created, %s updated' % (
            self.statistics['created'],
            self.statistics['updated']
        ))
