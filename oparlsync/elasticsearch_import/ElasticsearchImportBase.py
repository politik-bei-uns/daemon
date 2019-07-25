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


class ElasticsearchImportBase:
    def es_mapping_generator(self, base_object, deref=None, nested=False, delete=None):
        mapping = {}
        for field in base_object._fields:
            if delete:
                if hasattr(base_object._fields[field], delete):
                    continue
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
                    'type': 'text'
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
            result['type'] = 'text'
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
            result['type'] = 'text'
            result['fielddata'] = True
            if hasattr(field, 'fulltext'):
                result['analyzer'] = 'default_analyzer'
            if hasattr(field, 'sortable'):
                result['fields']['sort'] = {
                    'type': 'text',
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
            'type': 'text'
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
                    'tokenizer': {
                        'autocomplete': {
                            "type": "edge_ngram",
                            "min_gram": 2,
                            "max_gram": 10,
                            "token_chars": [
                                "letter"
                            ]
                        }
                    },
                    'analyzer': {
                        # Der Standard-Analyzer, welcher case-insensitive Volltextsuche bietet
                        'default_analyzer': {
                            'type': 'custom',
                            'tokenizer': 'standard',
                            'filter': [
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
                        'suggest_import_analyzer': {
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
                        'suggest_search_analyzer': {
                            'tokenizer': 'keyword',
                            'filter': [
                                'lowercase',
                                'asciifolding',
                                'german_stemmer'
                            ],
                            'char_filter': [
                                'html_strip'
                            ]
                        },
                        'autocomplete_import_analyzer': {
                            'tokenizer': 'autocomplete',
                            "filter": [
                                "lowercase"
                            ]
                        },
                        'autocomplete_search_analyzer': {
                            "tokenizer": "lowercase"
                        }
                    }
                }
            }
        }

    def generate_stopword_list(self):
        return []
