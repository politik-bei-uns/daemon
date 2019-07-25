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

import re
import geojson
import datetime
from ..base_task import BaseTask
from ..models import Street, Body, File, Location, StreetNumber, LocationOrigin, Paper


class GenerateGeoreferences(BaseTask):
    name = 'GenerateGeoreferences'
    services = [
        'mongodb'
    ]

    def __init__(self, body_id):
        self.body_id = body_id
        super().__init__()

    def __del__(self):
        pass

    def run(self, body_id, *args):
        if not self.config.ENABLE_PROCESSING:
            return
        self.body_config = self.get_body_config(body_id)
        if not self.body_config:
            return
        self.body = Body.objects(uid=body_id).no_cache().first()
        if not self.body:
            return
        if not self.body.region:
            return

        self.assign_regions()
        self.fix_name_in_geojson()
        if 'geocoding' in self.body_config:
            if self.body_config['geocoding'] == False:
                return
        # TODO: find a way to geocode bodies with multible sub-regions
        if self.body.region.level_max != 10:
            return

        self.assign_locations_to_street_numbers()
        self.check_for_streets()


    street_number_regexp = re.compile('(\d+)(.*)')

    def assign_regions(self):
        for location in Location.objects(body=self.body, region__exists=False).timeout(False).no_cache().all():
            location.region = self.body.region
            location.save()

    def fix_name_in_geojson(self):
        for location in Location.objects(body=self.body).timeout(False).no_cache().all():
            if not location.geojson:
                continue
            if 'properties' not in location.geojson:
                location.geojson['properties'] = {}
            if 'name' not in location.geojson['properties']:
                if location.description:
                    location.geojson['properties']['name'] = location.description
                else:
                    location.geojson['properties']['name'] = ''
                    if location.streetAddress:
                        location.geojson['properties']['name'] += location.streetAddress
                    if location.streetAddress and (location.postalCode or location.locality):
                        location.geojson['properties']['name'] += ', '
                    if location.postalCode:
                        location.geojson['properties']['name'] += location.postalCode + ' '
                    if location.locality:
                        location.geojson['properties']['name'] += location.locality
                location.save()

    def assign_locations_to_street_numbers(self):
        for location in Location.objects(body=self.body).timeout(False).no_cache().all():
            if location.streetAddress and not (location.street or location.streetNumber):
                street_name_str, street_number_str = self.get_address_parts(location.streetAddress)
                if not street_name_str or not street_number_str:
                    continue
                street_number = StreetNumber.objects(streetName__iexact=street_name_str, streetNumber__iexact=street_number_str, region=self.body.region).no_cache().first()
                if not street_number:
                    street_number_check = self.street_number_regexp.match(street_number_str)
                    if street_number_check.group(2):
                        street_number = StreetNumber.objects(streetName__iexact=street_name_str, streetNumber__iexact=street_number_check.group(1)).no_cache().first()
                        if not street_number:
                            print('%s %s not found' % (street_name_str, street_number_str))
                            continue
                    else:
                        print('%s %s not found' % (street_name_str, street_number_str))
                        continue

                location.type = 'address'
                location.streetName = street_number.streetName
                if street_number.postalCode:
                    location.postalCode = street_number.postalCode
                if street_number.subLocality:
                    location.subLocality = street_number.subLocality
                if street_number.locality:
                    location.locality = street_number.locality
                location.geojson = street_number.geojson
                location.streetNumber = street_number
                location.save()
                street_number.location = location
                street_number.save()

    def check_for_streets(self):
        streets = Street.objects(region=self.body.region).no_cache().timeout(False).all()
        for street in streets:
            # todo: use ES index to have aliases like str. -> strasse
            files = File.objects(body=self.body, georeferencesGenerated__exists=False).search_text('"' + street.streetName + '"').no_cache().timeout(False).all()
            for file in files:
                locations = []
                text = []
                if file.name:
                    text.append(file.name)
                if file.text:
                    text.append(file.text)
                if len(text):
                    text = ' '.join(text)
                    if street.streetName in text:
                        locations = self.check_for_street_numbers(street, text, file.id)
                # use whole street
                if not len(locations):
                    self.datalog.debug('Street %s found in File %s' % (street.streetName, file.id))
                    locations.append(self.create_location(
                        'street',
                        street.streetName,
                        None,
                        street.postalCode,
                        street.subLocality,
                        street.locality,
                        street.geojson,
                        streetObj=street
                    ))
                for paper in file.paper:
                    for location in locations:
                        save_paper = False
                        if location not in paper.location:
                            paper.location.append(location)
                            save_paper = True
                        if paper not in location.paper:
                            location.paper.append(paper)
                            location.save()
                        if not LocationOrigin.objects(paper=paper.id, location=location.id, origin='auto').no_cache().count():
                            location_origin = LocationOrigin()
                            location_origin.location = location.id
                            location_origin.paper = paper.id
                            location_origin.origin = 'auto'
                            location_origin.save()
                            if location_origin.id not in paper.locationOrigin:
                                paper.locationOrigin.append(location_origin.id)
                                save_paper = True
                        if save_paper:
                            paper.save()

                file.georeferencesStatus = 'generated'
                file.georeferencesGenerated = datetime.datetime.now()
                file.save()

    def check_for_street_numbers(self, street, text, file_id):
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
                    self.datalog.debug('Adresse %s %s found in File %s' % (street_number.streetName, street_number.streetNumber, file_id))
                    locations.append(self.create_location(
                        'address',
                        street_number.streetName,
                        street_number.streetNumber,
                        street_number.postalCode,
                        street_number.subLocality,
                        street_number.locality,
                        street_number.geojson
                    ))
        return locations

    def create_location(self, type, streetName, streetNumber, postalCode, subLocality, locality, geojson, streetObj=None, streetNumberObj=None):
        base_query = {
            'region': self.body.region,
            'streetAddress': streetName + ('' if type == 'street' else ' ' + streetNumber)
        }

        query = base_query.copy()

        query['locationType'] = type
        """
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
        """
        location = Location.objects(**base_query).no_cache().timeout(False).first()
        if location:
            if not location.body:
                location.body = []
            if self.body not in location.body:
                location.body.append(self.body)
        else:
            location = Location()
            query['autogenerated'] = True
            location.created = datetime.datetime.now()
            location.region = self.body.region
            location.body = [self.body]
        for key, value in query.items():
            setattr(location, key, value)
        location.modified = datetime.datetime.now()
        location.geojson = geojson
        if not location.locality:
            location.locality = self.body.region.name
        if type == 'street':
            if streetObj:
                location.street = streetObj
        elif type == 'address':
            if streetNumberObj:
                location.streetNumber = streetNumberObj
        location.save()
        if type == 'street':
            if streetObj:
                streetObj.location = location
                streetObj.save()
        elif type == 'address':
            if streetNumberObj:
                streetNumberObj.location = location
                streetNumberObj.save()

        return location

    street_regexp = re.compile('(.*?)\s*(\d+)\s*(.*)')

    def get_address_parts(self, text):
        match = self.street_regexp.match(self.fix_address_text(text))
        if match:
            name = match.group(1)
            number = match.group(2)
            if match.group(3):
                number += match.group(3).lower()
            return name, number
        return False, False

    def fix_address_text(self, text):
        text = text.replace('str.', 'straße')
        text = text.replace('strasse', 'straße')
        return text