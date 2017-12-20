# encoding: utf-8

"""
Copyright (c) 2017, Ernesto Ruge
All rights reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import osmium


class StreetCollector(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        self.street_fragments = []
        self.addresses = []
        self.nodes = {}

    def way(self, way):
        if way.tags.get('highway', None) and way.tags.get('name', None):
            street_fragment = {
                'osmid': way.id,
                'name': way.tags.get('name', None),
                'nodes': []
            }
            if way.tags.get('postal_code', None):
                street_fragment['postal_code'] = way.tags.get('postal_code', None)
            for node in way.nodes:
                street_fragment['nodes'].append(node.ref)
            self.street_fragments.append(street_fragment)
        elif way.tags.get('addr:street', None) and way.tags.get('addr:housenumber', None):
            address = {
                'osmid': way.id,
                'name': way.tags.get('addr:street', None),
                'number': way.tags.get('addr:housenumber', None),
                'postal_code': way.tags.get('addr:postcode', None),
                'sub_locality': way.tags.get('addr:suburb', None),
                'locality': way.tags.get('addr:city', None),
                'nodes': []
            }
            for node in way.nodes:
                address['nodes'].append(node.ref)
            self.addresses.append(address)

    def node(self, node):
        if node:
            self.nodes[node.id] = [node.location.lon, node.location.lat]
