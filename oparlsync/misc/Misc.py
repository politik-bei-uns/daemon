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

import os
import sys
import json
from _datetime import datetime
from geojson import Feature
import requests
import subprocess
import elasticsearch
from ..models import *
from ..elasticsearch_import import ElasticsearchImport
from minio.error import ResponseError


class Misc():
    def __init__(self, main):
        self.main = main
        self.valid_objects = [
            Body,
            LegislativeTerm,
            Organization,
            Person,
            Membership,
            Meeting,
            AgendaItem,
            Consultation,
            Paper,
            File,
            Location
        ]

    def run(self, body_id, *args):
        if not self.main.config.ENABLE_PROCESSING:
            return
        self.body = Body.objects(uid=body_id).no_cache().first()
        if not self.body:
            return

        statistics = {
            'objects': {
                'legislative_term': LegislativeTerm.objects(body=self.body).no_cache().count(),
                'organization': Organization.objects(body=self.body).no_cache().count(),
                'person': Person.objects(body=self.body).no_cache().count(),
                'membership': Membership.objects(body=self.body).no_cache().count(),
                'meeting': Meeting.objects(body=self.body).no_cache().count(),
                'agenda_item': AgendaItem.objects(body=self.body).no_cache().count(),
                'consultation': Consultation.objects(body=self.body).no_cache().count(),
                'paper': Paper.objects(body=self.body).no_cache().count(),
                'file': File.objects(body=self.body).no_cache().count(),
                'location': Location.objects(body=self.body).no_cache().count()
            }
        }
        body.statistics = statistics
        body.save()
