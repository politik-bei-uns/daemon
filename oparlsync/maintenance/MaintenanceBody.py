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
from ..models import *


class MaintenanceBody:
    def sync_bodies(self):
        bodies = os.listdir(self.config.BODY_DIR)
        for body in bodies:
            if body[-4:] == 'json':
                self.body_config = self.get_body_config(filename=body)
                self.sync_body(self.body_config['id'])

    def sync_body(self, body_id):
        if not self.body_config:
            self.body_config = self.get_body_config(body_id)
        query = {
            'uid': body_id
        }
        object_json = {
            '$set': {
                'uid': body_id,
                'rgs': self.body_config['rgs']
            }
        }
        if self.config.ENABLE_PROCESSING:
            region = Region.objects(rgs=self.body_config['rgs']).first()
            if region:
                object_json['$set']['region'] = region.id
        self.db_raw.body.find_one_and_update(
            query,
            object_json,
            upsert=True
        )

    def delete_last_sync(self, body):
        query = {}
        if body != 'all':
            query['uid'] = body

        object_json = {
            '$unset': {
                'lastSync': ''
            }
        }
        self.db_raw.body.update_many(
            query,
            object_json
        )

