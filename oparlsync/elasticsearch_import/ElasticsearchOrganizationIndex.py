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


class ElasticsearchOrganizationIndex:

    def organization_index(self):
        if not self.es.indices.exists_alias(name='organization-latest'):
            now = datetime.utcnow()
            index_name = 'organization-' + now.strftime('%Y%m%d-%H%M')

            mapping = self.es_mapping_generator(Organization, 'deref_organization')
            mapping['properties']['region'] = {
                'type': 'text'
            }

            self.es.indices.create(index=index_name, body={
                'settings': self.es_settings(),
                'mappings': mapping
            })

            self.es.indices.update_aliases({
                'actions': {
                    'add': {
                        'index': index_name,
                        'alias': 'organization-latest'
                    }
                }
            })
        else:
            index_name = list(self.es.indices.get_alias('organization-latest'))[0]

        regions = []
        region = self.body.region
        while (region):
            regions.append(str(region.id))
            region = region.parent

        for organization in Organization.objects(body=self.body).no_cache():
            if organization.deleted:
                self.es.delete(
                    index=index_name,
                    id=str(organization.id),
                    ignore=[400, 404]
                )
                continue
            organization_dict = organization.to_dict(deref='deref_organization', format_datetime=True, delete='delete_organization', clean_none=True)
            organization_dict['body_name'] = organization.body.name
            organization_dict['region'] = regions
            organization_dict['legacy'] = 'legacy' in organization_dict

            new_doc = self.es.index(
                index=index_name,
                id=str(organization.id),
                body=organization_dict
            )
            if new_doc['result'] in ['created', 'updated']:
                self.statistics[new_doc['result']] += 1
            else:
                self.datalog.warn('Unknown result at %s' % organization.id)
        self.datalog.info('ElasticSearch organization import successfull: %s created, %s updated' % (
            self.statistics['created'],
            self.statistics['updated']
        ))

