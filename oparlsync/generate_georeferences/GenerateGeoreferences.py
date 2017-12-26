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

import geojson
import datetime
from ..models import Street, Body, File, Location


class GenerateGeoreferences():
    def __init__(self, main):
        self.main = main

    def __del__(self):
        pass

    def run(self, body_id, *args):
        if not self.main.config.ENABLE_PROCESSING:
            return
        self.body_config = self.main.get_body_config(body_id)
        self.body = Body.objects(originalId=self.body_config['url']).first()

        streets = Street.objects(body=self.body).all()
        for street in streets:
            files = File.objects(body=self.body, georeferencesStatus__exists=False ).search_text(
                '"' + street.streetName + '"').all()
            for file in files:
                self.main.datalog.debug('Street %s found in File %s' % (street.streetName, file.id))
                locations = []
                if file.name:
                    if street.streetName in file.name:
                        locations += self.check_for_street_numbers(file, street, file.name)
                if file.text:
                    if street.streetName in file.text:
                        for location in self.check_for_street_numbers(file, street, file.text):
                            if location not in locations:
                                locations.append(location)
                # use whole street
                if not len(locations):
                    locations.append(self.create_location(
                        file,
                        'street',
                        street.streetName,
                        None,
                        street.postalCode,
                        street.subLocality,
                        street.locality,
                        street.geojson
                    ))
                for paper in file.paper:
                    for location in locations:
                        if location not in paper.location:
                            paper.location.append(location)
                            paper.save()
                        if paper not in location.paper:
                            location.paper.append(paper)
                            location.save()

                file.georeferencesStatus = 'generated'
                file.georeferencesGenerated = datetime.datetime.now()
                file.save()

    def check_for_street_numbers(self, file, street, text):
        locations = []
        if street.streetNumber:
            for street_number in street.streetNumber:
                if street_number.streetName + ' ' + street_number.streetNumber in text:
                    do_save = True
                    # check: is there another street number like examplestreet 38 if this is examplestreet 3? then proceed
                    # todo: make it even better detecting examplestreet 3 AND 38
                    for street_number_check in street.streetNumber:
                        if street_number.streetName + ' ' + street_number.streetNumber in street_number_check.streetName + ' ' + street_number_check.streetNumber and len(street_number.streetNumber) < len(street_number_check.streetNumber):
                            if street_number_check.streetName + ' ' + street_number_check.streetNumber in text:
                                do_save = False
                                continue
                    if not do_save:
                        continue
                    locations.append(self.create_location(
                        file,
                        'address',
                        street_number.streetName,
                        street_number.streetNumber,
                        street_number.postalCode,
                        street_number.subLocality,
                        street_number.locality,
                        street_number.geojson
                    ))
        return locations

    def create_location(self, file, type, streetName, streetNumber, postalCode, subLocality, locality, geojson):
        query = {
            'streetAddress': streetName,
            'autogenerated': True
        }
        if streetNumber:
            query['streetAddress'] = query['streetAddress'] + ' ' + streetNumber
            query['locationType'] = 'address'
        else:
            query['locationType'] = 'street'
        if postalCode:
            if type == 'street':
                if len(postalCode):
                    query['postalCode'] = postalCode[0]
            else:
                query['postalCode'] = postalCode
        if subLocality:
            if type == 'street':
                if len(subLocality):
                    query['subLocality'] = subLocality[0]
            else:
                query['subLocality'] = subLocality
        if locality:
            if type == 'street':
                if len(locality):
                    query['locality'] = locality[0]
            else:
                query['locality'] = locality

        location = Location.objects(**query).no_cache()
        if location.count():
            location = location.first()
            if location.body:
                if len(location.body):
                    if self.body not in location.body:
                        location.body.append(self.body)
        else:
            location = Location()
            location.created = datetime.datetime.now()
            location.body = [self.body]
        for key, value in query.items():
            setattr(location, key, value)
        location.modified = datetime.datetime.now()
        location.geojson = geojson
        location.save()
        return location
