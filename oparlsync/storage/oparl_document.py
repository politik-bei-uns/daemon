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

from mongoengine import Document, BooleanField, ReferenceField, DateTimeField, StringField, ListField, DecimalField, \
    GeoJsonBaseField


class OParlDocument(object):
    def to_dict(self, deref=None, format_datetime=False, delete=False, clean_none=False):
        result = {}
        for field in self._fields:
            if delete:
                if hasattr(self._fields[field], delete):
                    if getattr(self._fields[field], delete):
                        continue
            if clean_none and hasattr(object, field):
                if getattr(object, field):
                    continue
            if self._fields[field].__class__.__name__ == 'ListField':
                if self._fields[field].field.__class__.__name__ == 'ReferenceField':
                    if not field in result:
                        result[field] = []
                    if deref:
                        if getattr(self._fields[field].field, deref):
                            for sub_object in getattr(self, field):
                                result[field].append(
                                    sub_object.to_dict(deref=deref, format_datetime=format_datetime, delete=delete))
                        else:
                            for sub_object in getattr(self, field):
                                result[field].append(str(sub_object.id))
                    else:
                        for sub_object in getattr(self, field):
                            result[field].append(str(sub_object.id))
                else:
                    result[field] = getattr(self, field)
            elif self._fields[field].__class__.__name__ == 'ReferenceField':
                if getattr(self, field):
                    if deref:
                        if getattr(self._fields[field], deref):
                            result[field] = getattr(self, field).to_dict(deref=deref, format_datetime=format_datetime,
                                                                         delete=delete)
                        else:
                            result[field] = str(getattr(self, field).id)
                    else:
                        result[field] = str(getattr(self, field).id)
                else:
                    result[field] = str(getattr(self, field))
            elif self._fields[field].__class__.__name__ == 'ObjectIdField':
                result[field] = str(getattr(self, field))
            else:
                if getattr(self, field) != None:
                    if format_datetime:
                        if self._fields[field].__class__.__name__ == 'DateTimeField':
                            if self._fields[field].datetime_format == 'datetime':
                                result[field] = getattr(self, field).strftime('%Y-%m-%dT%H:%M:%S')
                            else:
                                result[field] = getattr(self, field).strftime('%H:%M:%S')
                                if result[field] == '00:00:00':
                                    result[field] = None
                        else:
                            result[field] = getattr(self, field)
                    else:
                        result[field] = getattr(self, field)
            if field in result and clean_none:
                if result[field] == 'None':
                    del result[field]
        return result
