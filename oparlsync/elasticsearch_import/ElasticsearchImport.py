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
from ..base_task import BaseTask
from .ElasticsearchImportBase import ElasticsearchImportBase
from .ElasticsearchStreetIndex import ElasticsearchStreetIndex
from .ElasticsearchPaperIndex import ElasticsearchPaperIndex
from .ElasticsearchPaperLocationIndex import ElasticsearchPaperLocationIndex
from .ElasticsearchOrganizationIndex import ElasticsearchOrganizationIndex
from .ElasticsearchPersonIndex import ElasticsearchPersonIndex


class ElasticsearchImport(BaseTask, ElasticsearchImportBase, ElasticsearchStreetIndex, ElasticsearchPaperIndex,
                          ElasticsearchPaperLocationIndex, ElasticsearchOrganizationIndex, ElasticsearchPersonIndex):
    name = 'ElasticsearchImport'
    services = [
        'mongodb',
        'elasticsearch'
    ]

    def __init__(self, body_id):
        self.body_id = body_id
        super().__init__()

    def __del__(self):
        pass

    def run(self, body_id, *args):
        if not (self.config.ENABLE_PROCESSING and self.config.ES_ENABLED):
            return
        self.body = Body.objects(uid=body_id).no_cache().first()
        if not self.body:
            return
        self.statistics = {
            'created': 0,
            'updated': 0
        }
        self.street_index()
        self.paper_location_index()
        self.paper_index()
        self.body = None
        self.es = None
