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

from mongoengine import Document, EmbeddedDocument, BooleanField, ReferenceField, DateTimeField, IntField, StringField, \
    ListField, DecimalField, DictField
from .oparl_document import OParlDocument


class File(Document, OParlDocument):
    meta = {
        'indexes': [
            {
                'fields': ['$name', "$text", 'body'],
                'default_language': 'english',
                'weights': {'name': 10, 'text': 2}
            }
        ]
    }

    type = 'https://schema.oparl.org/1.0/File'
    body = ReferenceField('Body', dbref=False, deref_paper_location=False, deref_paper=False)
    name = StringField(fulltext=True)
    fileName = StringField()
    mimeType = StringField()
    date = DateTimeField(datetime_format='date')
    size = DecimalField()
    sha1Checksum = StringField()
    sha512Checksum = StringField()
    text = StringField(fulltext=True)
    accessUrl = StringField()
    downloadUrl = StringField()
    externalServiceUrl = StringField()
    masterFile = ReferenceField('File', dbref=False, deref_paper_location=False, deref_paper=False)
    derivativeFile = ListField(ReferenceField('File', dbref=False, deref_paper_location=False, deref_paper=False), default=[])
    fileLicense = StringField()
    meeting = ListField(ReferenceField('Meeting', dbref=False, deref_paper_location=False, deref_paper=False), default=[])
    agendaItem = ListField(ReferenceField('AgendaItem', dbref=False, deref_paper_location=False, deref_paper=False), default=[])
    paper = ListField(ReferenceField('Paper', dbref=False, deref_paper_location=False, deref_paper=False), default=[])
    license = StringField()
    keyword = ListField(StringField(fulltext=True), default=[])
    created = DateTimeField(datetime_format='datetime')
    modified = DateTimeField(datetime_format='datetime')
    web = StringField()
    deleted = BooleanField()

    # Politik bei Uns Felder
    legacy = BooleanField(vendor_attribute=True)
    downloaded = BooleanField(vendor_attribute=True)
    originalId = StringField(vendor_attribute=True)
    mirrorId = StringField(vendor_attribute=True)
    storedAtMirror = BooleanField(vendor_attribute=True)
    mirrorDownloadUrl = StringField(vendor_attribute=True)
    mirrorAccessUrl = StringField(vendor_attribute=True)
    originalWeb = StringField(vendor_attribute=True)
    originalAccessUrl = StringField(vendor_attribute=True)
    originalDownloadUrl = StringField(vendor_attribute=True)
    textGenerated = DateTimeField(datetime_format='datetime', vendor_attribute=True, delete_paper=True, delete_paper_location=True)
    textStatus = StringField(vendor_attribute=True, delete_paper=True, delete_paper_location=True)
    thumbnailGenerated = DateTimeField(datetime_format='datetime', vendor_attribute=True, delete_paper=True, delete_paper_location=True)
    thumbnailStatus = StringField(vendor_attribute=True, delete_paper=True, delete_paper_location=True)
    georeferencesGenerated = DateTimeField(datetime_format='datetime', vendor_attribute=True, delete_paper=True, delete_paper_location=True)
    georeferencesStatus = StringField(vendor_attribute=True, delete_paper=True, delete_paper_location=True)
    thumbnail = DictField(vendor_attribute=True, delete_paper=True, delete_paper_location=True)
    pages = IntField(vendor_attribute=True, delete_paper=True, delete_paper_location=True)
    keywordUsergenerated = ListField(ReferenceField('KeywordUsergenerated', deref_paper_location=False, deref_paper=False), vendor_attribute=True)

    # Felder zur Verarbeitung
    _object_db_name = 'file'
    _attribute = 'file'

    def __init__(self, *args, **kwargs):
        super(Document, self).__init__(*args, **kwargs)

    def __repr__(self):
        return '<File %r>' % self.name
