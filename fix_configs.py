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
import json
from yaml import dump


for configfile in os.listdir('./bodies'):
    with open('bodies/%s' % configfile) as body_config_file:
        configdata = json.load(body_config_file)
    if 'osm_relation' in configdata:
        del configdata['osm_relation']
    if 'geofabrik_package' in configdata:
        del configdata['geofabrik_package']
    if 'force_full_sync' in configdata:
        del configdata['force_full_sync']
    if not configdata.get('name', ''):
        continue
    configdata['id'] = 'DE-%s' % configdata['id']
    name = configdata.get('name', '').lower().replace(' ', '-')
    name = name.replace('ü', 'ue').replace('ä', 'ae').replace('ö', 'oe').replace('ß', 'ss')
    with open('bodies/%s-%s.yml' % (configdata.get('id', ''), name), 'w') as body_config_file:
        body_config_file.write(dump(configdata))


for configfile in os.listdir('./regions'):
    with open('regions/%s' % configfile) as region_config_file:
        configdata = json.load(region_config_file)
    if not configdata.get('name', ''):
        continue
    configdata['country'] = 'DE'
    configdata['id'] = 'DE-%s' % configdata['rgs']
    name = configdata.get('name', '').lower().replace(' ', '-')
    name = name.replace('ü', 'ue').replace('ä', 'ae').replace('ö', 'oe').replace('ß', 'ss')
    with open('regions/DE-%s-%s.yml' % (configdata.get('rgs', ''), name), 'w') as region_config_file:
        region_config_file.write(dump(configdata))