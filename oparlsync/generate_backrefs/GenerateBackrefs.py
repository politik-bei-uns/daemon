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

from ..models import *


class GenerateBackrefs():
    def __init__(self, main):
        self.main = main

    def __del__(self):
        pass

    def run(self, body_id, *args):
        self.body_config = self.main.get_body_config(body_id)
        self.body = Body.objects(originalId=self.body_config['url']).first()
        self.backrefs_created = 0

        # Organization
        #self.backref_item(Membership, 'organization', 'membership')
        self.backref_list_single(Organization, 'membership', 'organization')

        # File
        self.backref_item(Meeting, 'invitation', 'meeting')
        self.backref_item(Meeting, 'resultsProtocol', 'meeting')
        self.backref_item(Meeting, 'verbatimProtocol', 'meeting')
        self.backref_list(Meeting, 'auxiliaryFile', 'meeting')
        self.backref_list(Meeting, 'auxiliaryFile', 'meeting')

        self.backref_item(AgendaItem, 'resolutionFile', 'agendaItem')
        self.backref_list(AgendaItem, 'auxiliaryFile', 'agendaItem')
        self.backref_single(AgendaItem, 'consultation', 'agendaItem')

        self.backref_item(Paper, 'mainFile', 'paper')
        self.backref_list(Paper, 'auxiliaryFile', 'paper')
        self.backref_list_single(Paper, 'consultation', 'paper')


        # Location
        self.backref_item(Person, 'location', 'person')
        self.backref_item(Organization, 'location', 'organization')
        self.backref_item(Meeting, 'location', 'meeting')
        self.backref_list(Paper, 'location', 'paper')

        self.main.datalog.info('Created %s backreferences' % self.backrefs_created)

    def backref_single(self, obj_name, attr, backref_attr):
        filter = {'body': self.body, attr + '__exists': True}
        for obj in obj_name.objects(**filter).no_cache().all():
            if obj != getattr(getattr(obj, attr), backref_attr):
                setattr(getattr(obj, attr), backref_attr, obj)
                getattr(obj, attr).save()
                self.backrefs_created += 1

    def backref_item(self, obj_name, attr, backref_attr):
        filter = {'body': self.body, attr + '__exists': True}
        for obj in obj_name.objects(**filter).no_cache().all():
            if obj not in getattr(getattr(obj, attr), backref_attr):
                getattr(getattr(obj, attr), backref_attr).append(obj)
                getattr(obj, attr).save()
                self.backrefs_created += 1

    def backref_list(self, obj_name, attr, backref_attr):
        filter = {'body': self.body, attr + '__exists': True}
        for obj in obj_name.objects(**filter).no_cache().all():
            for sub_obj in getattr(obj, attr):
                if obj not in getattr(sub_obj, backref_attr):
                    getattr(sub_obj, backref_attr).append(obj)
                    sub_obj.save()
                    self.backrefs_created += 1

    def backref_list_single(self, obj_name, attr, backref_attr):
        filter = {'body': self.body, attr + '__exists': True}
        for obj in obj_name.objects(**filter).no_cache().all():
            for sub_obj in getattr(obj, attr):
                if obj != getattr(sub_obj, backref_attr):
                    setattr(sub_obj, backref_attr, obj)
                    sub_obj.save()
                    self.backrefs_created += 1
