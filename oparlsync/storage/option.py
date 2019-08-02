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

import pytz
from decimal import Decimal
from datetime import datetime
from mongoengine import Document, DateTimeField, StringField


class Option(Document):
    created = DateTimeField(default=datetime.utcnow())
    modified = DateTimeField(default=datetime.utcnow())
    key = StringField()
    value = StringField()
    type = StringField()


    @classmethod
    def get(cls, key, default=None):
        option = cls.objects(key=key).first()
        output = cls.get_output_value(option)
        if default is not None and not output:
            return default
        return output

    @classmethod
    def get_output_value(cls, option):
        if not option:
            return
        if not option.type or option.type == 'string':
            return option.value
        elif option.type == 'integer':
            return int(option.value)
        elif option.type == 'decimal':
            return Decimal(option.value)
        elif option.type == 'datetime':
            return datetime.strptime(option.value, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.UTC)
        elif option.type == 'date':
            return datetime.strptime(option.value, '%Y-%m-%d').date()

    @classmethod
    def set(cls, key, value, value_type='string'):
        option = cls.objects(key=key).first()
        if option:
            if value == cls.get_output_value(option) and value_type == option.type:
                return
        else:
            option = cls()
            option.key = key
        option.modified = datetime.utcnow()
        option.type = value_type
        if value_type == 'string':
            option.value = value
        elif value_type == 'decimal' or value_type == 'integer':
            option.value = str(value)
        elif option.type == 'datetime':
            option.value = value.strftime('%Y-%m-%dT%H:%M:%S')
        elif option.type == 'date':
            option.value = value.strftime('%Y-%m-%d')
        option.save()

    @classmethod
    def remove(cls, key):
        option = cls.objects(key=key).first()
        if not option:
            return
        option.save()