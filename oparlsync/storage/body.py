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
    GeoJsonBaseField, DictField
from .oparl_document import OParlDocument


class Body(Document, OParlDocument):
    meta = {
        'indexes': [
            'originalId'
        ],
    }

    type = 'https://schema.oparl.org/1.1/Body'
    shortName = StringField()
    name = StringField()
    website = StringField()
    license = StringField()
    licenseValidSince = DateTimeField(datetime_format='date')
    oparlSince = DateTimeField(datetime_format='date')
    ags = StringField()
    rgs = StringField()
    equivalent = ListField(StringField(), default=[])
    contactEmail = StringField()
    contactName = StringField()
    legislativeTerm = ListField(ReferenceField('LegislativeTerm', dbref=False, internal_output=True), default=[])
    classification = StringField()
    location = ReferenceField('Location', dbref=False, internal_output=True)
    keyword = ListField(StringField(), default=[])
    created = DateTimeField(datetime_format='datetime')
    modified = DateTimeField(datetime_format='datetime')
    web = StringField()
    deleted = BooleanField()

    # Politik bei Uns Felder
    region = ReferenceField('Region', vendor_attribute=True)
    uid = StringField(vendor_attribute=True)
    legacy = BooleanField(vendor_attribute=True)
    originalId = StringField(vendor_attribute=True)
    mirrorId = StringField(vendor_attribute=True)
    lastSync = DateTimeField(datetime_format='datetime', vendor_attribute=True)
    beforeLastSync = DateTimeField(datetime_format='datetime', vendor_attribute=True)
    statistics = DictField(vendor_attribute=True)

    # Felder zur Verarbeitung
    _object_db_name = 'body'
    _attribute = 'body'

    def __init__(self, *args, **kwargs):
        super(Document, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<Body %r>' % self.name
