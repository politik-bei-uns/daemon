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


class MaintenanceGeo:
    def update_street_locality(self):
        for street in Street.objects():
            if not len(street.locality):
                street.locality = [street.body.name]
                street.geojson['properties']['locality'] = [street.body.name]
                street.save()
                print('modified %s' % street.streetName)
        for street in StreetNumber.objects():
            if not street.locality:
                street.locality = street.body.name
                if 'properties' not in street.geojson:
                    street.geojson['properties'] = {}
                street.geojson['properties']['locality'] = street.body.name
                street.save()
                print('modified %s' % street.streetName)
        for location in Location.objects():
            if not location.locality:
                location.locality = location.body[0].name
                if 'properties' not in location.geojson:
                    location.geojson['properties'] = {}
                location.geojson['properties']['locality'] = location.body[0].name

    def delete_all_locations(self, body):
        Location.objects.delete()
        LocationOrigin.objects.delete()

        query = {}
        if body != 'all':
            query['uid'] = body
        object_json = {
            '$unset': {
                'location': '',
                'locationOrigin': ''
            }
        }
        self.db_raw.body.update_many(
            query,
            object_json
        )
        self.db_raw.person.update_many(
            query,
            object_json
        )
        self.db_raw.organization.update_many(
            query,
            object_json
        )
        self.db_raw.meeting.update_many(
            query,
            object_json
        )
        self.db_raw.paper.update_many(
            query,
            object_json
        )
        self.db_raw.street.update_many(
            query,
            object_json
        )
        self.db_raw.street_number.update_many(
            query,
            object_json
        )

    def reset_generate_georeferences(self, body_uid):
        query = {}
        if body_uid != 'all':
            body = Body.objects(uid=body_uid).first()
            if not body:
                return
            query['body'] = body.id

        object_json = {
            '$unset': {
                'georeferencesGenerated': '',
                'georeferencesStatus': ''
            }
        }
        self.db_raw.file.update_many(
            query,
            object_json
        )
