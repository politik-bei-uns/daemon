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

import sys
from ..models import *
from ..base_task import BaseTask
from mongoengine.errors import ValidationError

from .MaintenanceRemove import MaintenanceRemove
from .MaintenanceRegion import MaintenanceRegion
from .MaintenanceRegionElastic import MaintenanceRegionElastic
from .MaintenanceSitemap import MaintenanceSitemap
from .MaintenanceBody import MaintenanceBody
from .MaintenanceGeo import MaintenanceGeo


class Maintenance(BaseTask, MaintenanceRemove, MaintenanceRegion, MaintenanceRegionElastic, MaintenanceSitemap,
                  MaintenanceBody, MaintenanceGeo):
    name = 'Maintenance'
    services = [
        'mongodb',
        's3',
        'elasticsearch'
    ]

    def __init__(self, body_id):
        self.body_id = body_id
        super().__init__()
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
        if len(args) < 1:
            sys.exit('usage: python manage.py maintenance $body_id remove|clean')

        if args[0] == 'remove':
            self.remove(body_id)
        elif args[0] == 'generate_regions':
            self.generate_regions()
        elif args[0] == 'elasticsearch_regions':
            self.elasticsearch_regions()
        elif args[0] == 'update_street_locality':
            self.update_street_locality()
        elif args[0] == 'sync_bodies':
            self.sync_bodies()
        elif args[0] == 'sync_body':
            self.sync_body(body_id)
        elif args[0] == 'delete_all_locations':
            self.delete_all_locations(body_id)
        elif args[0] == 'delete_last_sync':
            self.delete_last_sync(body_id)
        elif args[0] == 'reset_generate_georeferences':
            self.reset_generate_georeferences(body_id)
        elif args[0] == 'sitemap_master':
            self.sitemap_master()
        elif args[0] == 'fix_oparl_11':
            self.fix_oparl_11()
        elif args[0] == 'migrate_new_ids':
            self.migrate_new_ids()
        elif args[0] == 'test':
            self.test()
        else:
            sys.exit('unknown task')

    def activate_body(self, body_id):
        self.sync_body(body_id)
        self.generate_regions()
        self.sync_body(body_id)
        self.elasticsearch_regions()

    def fix_oparl_11(self):
        count_delete = 0
        count_update = 0
        count_invalid = 0
        objects = [Body, Person, Membership, Organization, Meeting, AgendaItem, Paper, Consultation, File, Location, LegislativeTerm]
        for obj in objects:
            for item in getattr(obj, 'objects'):
                if not item.originalId:
                    continue
                if '/webservice/oparl/v1.0/' in item.originalId:
                    item.delete()
                    count_delete += 1
                if '/webservice/oparl/v1/' in item.originalId:
                    paper.originalId = item.originalId.replace('/webservice/oparl/v1/', '/webservice/oparl/v1.1/')
                    try:
                        item.save()
                        count_update += 1
                    except ValidationError:
                        item.delete()
                        count_invalid += 1
        print('deleted %s datasets, deleted %s invalid datasets, updated %s datasets' % (count_delete, count_invalid, count_update))

    def migrate_new_ids(self):
        for body in Body.objects:
            if body.uid[0:3] == 'DE-':
                continue
            body.uid = 'DE-%s' % body.uid
            body.save()

    def test(self):
        for region in Region.objects:
            print(region.rgs)