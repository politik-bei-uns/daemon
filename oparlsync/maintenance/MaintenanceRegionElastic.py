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
from ..elasticsearch_import import ElasticsearchImport


class MaintenanceRegionElastic:
    def elasticsearch_regions(self):
        if not self.es.indices.exists_alias(name='region-latest'):
            now = datetime.utcnow()
            index_name = 'region-' + now.strftime('%Y%m%d-%H%M')

            es_import = ElasticsearchImport(self)

            mapping = es_import.es_mapping_generator(Region, deref='deref_region', delete='delete_region')
            mapping['properties']['body_count'] = {
                'type': 'integer'
            }
            self.es.indices.create(index=index_name, body={
                'settings': es_import.es_settings(),
                'mappings': {
                    'region': mapping
                }
            })

            self.es.indices.update_aliases({
                'actions': {
                    'add': {
                        'index': index_name,
                        'alias': 'region-latest'
                    }
                }
            })

        else:
            index_name = list(self.es.indices.get_alias('region-latest'))[0]

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
            region_dict['legacy']= bool(region.legacy)

            new_doc = self.es.index(
                index=index_name,
                id=str(region.id),
                doc_type='region',
                body=region_dict
            )