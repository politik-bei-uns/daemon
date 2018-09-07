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
from minio.error import ResponseError


class MaintenanceRemove:
    def remove(self, body_id):
        self.body_config = self.get_body_config(body_id)
        body = Body.objects(originalId=self.body_config['url']).first()
        if not body:
            sys.exit('body not found')
        # delete in mongodb

        for object in self.valid_objects:
            if object != Body:
                object.objects(body=body.id).delete()
        # delete in minio
        try:
            get_name = lambda object: object.object_name
            names = map(get_name, self.s3.list_objects_v2(self.config.S3_BUCKET, 'files/%s' % str(body.id),
                                                               recursive=True))
            for error in self.s3.remove_objects(self.config.S3_BUCKET, names):
                self.datalog.warn(
                    'Critical error deleting file from File %s from Body %s' % (error.object_name, body.id))
        except ResponseError as err:
            self.datalog.warn('Critical error deleting files from Body %s' % body.id)

        body.delete()