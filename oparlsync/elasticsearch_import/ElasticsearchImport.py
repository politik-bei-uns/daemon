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


class ElasticsearchImport():
    def __init__(self, main):
        self.main = main

    def __del__(self):
        pass

    def run(self, body_id, *args):
        if not self.main.config.ENABLE_PROCESSING:
            return
        self.body_config = self.main.get_body_config(body_id)
        self.body = Body.objects(originalId=self.body_config['url']).no_cache().first()
        self.statistics = {
            'created': 0,
            'updated': 0
        }
        self.street_index()
        self.paper_location_index()
        self.paper_index()

    def street_index(self):

        if not self.main.es.indices.exists_alias(name='street-latest'):
            now = datetime.utcnow()
            index_name = 'street-' + now.strftime('%Y%m%d-%H%M')

            self.main.es.indices.create(index=index_name, body={
                'settings': self.es_settings(),
                'mappings': {
                    'street': self.es_mapping_generator(Street, 'deref_street')
                }
            })

            self.main.es.indices.update_aliases({
                'actions': {
                    'add': {
                        'index': index_name,
                        'alias': 'street-latest'
                    }
                }
            })
        else:
            index_name = list(self.main.es.indices.get_alias('street-latest'))[0]

        for street in Street.objects(body=self.body).no_cache():
            street_dict = street.to_dict(deref='deref_street', format_datetime=True, delete='delete_street')
            if 'geojson' in street_dict:
                del street_dict['geojson']
            new_doc = self.main.es.index(
                index=index_name,
                id=str(street.id),
                doc_type='street',
                body=street_dict
            )
            if new_doc['result'] in ['created', 'updated']:
                self.statistics[new_doc['result']] += 1
            else:
                self.main.datalog.warn('Unknown result at %s' % street.id)
        self.main.datalog.info('ElasticSearch street import successfull: %s created, %s updated' % (
            self.statistics['created'], self.statistics['updated']))


    def paper_index(self):

        if not self.main.es.indices.exists_alias(name='paper-latest'):
            now = datetime.utcnow()
            index_name = 'paper-' + now.strftime('%Y%m%d-%H%M')

            mapping = self.es_mapping_generator(Paper, 'deref_paper')

            self.main.es.indices.create(index=index_name, body={
                'settings': self.es_settings(),
                'mappings': {
                    'paper': mapping
                }
            })

            self.main.es.indices.update_aliases({
                'actions': {
                    'add': {
                        'index': index_name,
                        'alias': 'paper-latest'
                    }
                }
            })

        else:
            index_name = list(self.main.es.indices.get_alias('paper-latest'))[0]

        for paper in Paper.objects(body=self.body).no_cache():
            paper_dict = paper.to_dict(deref='deref_paper', format_datetime=True, delete='delete_paper', clean_none=True)
            paper_dict['body_name'] = paper.body.name
            new_doc = self.main.es.index(
                index=index_name,
                id=str(paper.id),
                doc_type='paper',
                body=paper_dict
            )
            if new_doc['result'] in ['created', 'updated']:
                self.statistics[new_doc['result']] += 1
            else:
                self.main.datalog.warn('Unknown result at %s' % paper.id)
        self.main.datalog.info('ElasticSearch paper import successfull: %s created, %s updated' % (
            self.statistics['created'], self.statistics['updated']))

    def paper_location_index(self):

        if not self.main.es.indices.exists_alias(name='paper-location-latest'):
            now = datetime.utcnow()
            index_name = 'paper-location-' + now.strftime('%Y%m%d-%H%M')

            self.main.es.indices.create(index=index_name, body={
                'settings': self.es_settings(),
                'mappings': {
                    'location': self.es_mapping_generator(Location, 'deref_paper_location')
                }
            })

            self.main.es.indices.update_aliases({
                'actions': {
                    'add': {
                        'index': index_name,
                        'alias': 'paper-location-latest'
                    }
                }
            })

        else:
            index_name = list(self.main.es.indices.get_alias('paper-location-latest'))[0]

        for location in Location.objects(body=self.body).no_cache():
            location_dict = location.to_dict(deref='deref_paper_location', format_datetime=True, delete='delete_paper_location')
            if 'geojson' in location_dict:
                if location_dict['geojson']:
                    if 'geometry' in location_dict['geojson']:
                        location_dict['geosearch'] = location_dict['geojson']['geometry']
                        for field in ['paper']: #['person', 'organization', 'meeting', 'paper']:
                            if field + 's' in location_dict:
                                if type(location_dict[field + 's']) is list:
                                    if 'properties' not in location_dict['geojson']:
                                        location_dict['geojson']['properties'] = {}
                                    location_dict['geojson']['properties'][field + '-count'] = len(location_dict[field + 's'])
                    else:
                        del location_dict['geojson']
                else:
                    del location_dict['geojson']
            if 'geojson' in location_dict:
                location_dict['geosearch'] = location_dict['geojson']['geometry']
                location_dict['geotype'] = location_dict['geojson']['geometry']['type']
                location_dict['geojson'] = json.dumps(location_dict['geojson'])

            new_doc = self.main.es.index(
                index=index_name,
                id=str(location.id),
                doc_type='location',
                body=location_dict
            )
            if new_doc['result'] in ['created', 'updated']:
                self.statistics[new_doc['result']] += 1
            else:
                self.main.datalog.warn('Unknown result at %s' % location.id)
        self.main.datalog.info('ElasticSearch paper-location import successfull: %s created, %s updated' % (
        self.statistics['created'], self.statistics['updated']))

    def es_mapping_generator(self, base_object, deref=None, nested=False, delete=None):
        mapping = {}
        for field in base_object._fields:
            if base_object._fields[field].__class__.__name__ == 'ListField':
                if base_object._fields[field].field.__class__.__name__ == 'ReferenceField':
                    if getattr(base_object._fields[field].field, deref):
                        mapping[field] = self.es_mapping_generator(base_object._fields[field].field.document_type,
                                                                   deref, True)
                    else:
                        mapping[field] = self.es_mapping_field_object()
                else:
                    mapping[field] = self.es_mapping_field_generator(base_object._fields[field].field)
                    if mapping[field] == None:
                        del mapping[field]
            elif base_object._fields[field].__class__.__name__ == 'ReferenceField':
                if getattr(base_object._fields[field], deref):
                    mapping[field] = self.es_mapping_generator(base_object._fields[field].document_type, deref, True)
                else:
                    mapping[field] = self.es_mapping_field_object()
            elif hasattr(base_object._fields[field], 'geojson'):
                mapping['geosearch'] = {
                    'type': 'geo_shape'
                }
                mapping['geojson'] = {
                    'type': 'string'
                }
                mapping['geotype'] = {
                    'type': 'keyword'
                }
            else:
                mapping[field] = self.es_mapping_field_generator(base_object._fields[field])
            if not mapping[field]:
                del mapping[field]

        mapping = {
            'properties': mapping
        }
        if nested:
            mapping['type'] = 'nested'
        return mapping

    def es_mapping_field_generator(self, field):
        result = {'store': True}
        if field.__class__.__name__ == 'ObjectIdField':
            result['type'] = 'string'
        elif field.__class__.__name__ == 'IntField':
            result['type'] = 'integer'
        elif field.__class__.__name__ == 'DateTimeField':
            result['type'] = 'date'
            if field.datetime_format == 'datetime':
                result['format'] = 'date_hour_minute_second'
            elif field.datetime_format == 'date':
                result['format'] = 'date'
        elif field.__class__.__name__ == 'StringField':
            result['fields'] = {}
            result['type'] = 'string'
            if hasattr(field, 'fulltext'):
                result['index'] = 'analyzed'
                result['analyzer'] = 'default_analyzer'
            else:
                result['index'] = 'not_analyzed'
            if hasattr(field, 'sortable'):
                result['fields']['sort'] = {
                    'type': 'string',
                    'analyzer': 'sort_analyzer',
                    'fielddata': True
                }
        elif field.__class__.__name__ == 'BooleanField':
            result['type'] = 'boolean'
        else:
            return None
        return result

    def es_mapping_field_object(self):
        return {
            'fielddata': True,
            'type': 'string'
        }

    def es_settings(self):
        return {
            'index': {
                'max_result_window': 250000,
#                'mapping': {
#                    'nested_fields': {
#                        'limit': 500
#                    },
#                    'total_fields': {
#                        'limit': 2500
#                    }
#                },
                'analysis': {
                    'filter': {
                        'german_stop': {
                            "type": 'stop',
                            "stopwords": '_german_'
                        },
                        'german_stemmer': {
                            "type": 'stemmer',
                            "language": 'light_german'
                        },
                        'custom_stop': {
                            "type": 'stop',
                            'stopwords': self.generate_stopword_list()
                        }
                    },
                    'char_filter': {
                        'sort_char_filter': {
                            'type': 'pattern_replace',
                            'pattern': '"',
                            'replace': ''
                        }
                    },
                    'analyzer': {
                        # Der Standard-Analyzer, welcher case-insensitive Volltextsuche bietet
                        'default_analyzer': {
                            'type': 'custom',
                            'tokenizer': 'standard',
                            'filter': [
                                'standard',
                                'lowercase',
                                'custom_stop',
                                'german_stop',
                                'german_stemmer'
                            ]
                        },
                        'sort_analyzer': {
                            'tokenizer': 'keyword',
                            'filter': [
                                'lowercase',
                                'asciifolding',
                                'custom_stop',
                                'german_stop',
                                'german_stemmer'
                            ],
                            'char_filter': [
                                'sort_char_filter'
                            ]
                        },
                        'livesearch_import_analyzer': {
                            'tokenizer': 'keyword',
                            'filter': [
                                'lowercase',
                                'asciifolding',
                                'custom_stop',
                                'german_stop',
                                'german_stemmer'
                            ],
                            'char_filter': [
                                'html_strip'
                            ]
                        },
                        # Analyzer für die Live-Suche. Keine Stopwords, damit z.B. die -> diesel funktioniert
                        'livesearch_search_analyzer': {
                            'tokenizer': 'keyword',
                            'filter': [
                                'lowercase',
                                'asciifolding',
                                'german_stemmer'
                            ],
                            'char_filter': [
                                'html_strip'
                            ]
                        }
                    }
                }
            }
        }

    def generate_stopword_list(self):
        return []
