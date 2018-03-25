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
import datetime
import subprocess
from ..models import *
from ..base_task import BaseTask
from minio.error import ResponseError, NoSuchKey


class GenerateFulltext(BaseTask):
    name = 'GenerateFulltext'
    services = [
        'mongodb',
        's3'
    ]


    def __init__(self, body_id):
        self.body_id = body_id
        super().__init__()
        self.statistics = {
            'wrong-mimetype': 0,
            'file-missing': 0,
            'no-text': 0,
            'successful': 0
        }

    def run(self, body_id, *args):
        if not self.config.ENABLE_PROCESSING:
            return
        self.body = Body.objects(uid=body_id).no_cache().first()
        if not self.body:
            return

        files = File.objects(textStatus__exists=False, body=self.body.id).timeout(False).no_cache().all()
        for file in files:
            self.datalog.info('processing file %s' % file.id)
            file.modified = datetime.datetime.now()
            file.textGenerated = datetime.datetime.now()
            # get file
            file_path = os.path.join(self.config.TMP_FULLTEXT_DIR, str(file.id))
            if not self.get_file(file, file_path):
                self.datalog.warn('file not found: %s' % file.id)
                self.statistics['file-missing'] += 1
                file.textStatus = 'file-missing'
                file.save()
                continue

            # decide app based on mimetype
            if file.mimeType == 'application/pdf':
                cmd = '%s -nopgbrk -enc UTF-8 %s -' % (self.config.PDFTOTEXT_COMMAND, file_path)
            elif file.mimeType == 'application/msword':
                cmd = '%s --to=txt --to-name=fd://1 %s' % (self.config.ABIWORD_COMMAND, file_path)
            else:
                cmd = None
                self.statistics['wrong-mimetype'] += 1
                file.textStatus = 'bad-mimetype'
                file.save()
                os.unlink(file_path)
                continue

            text = self.execute(cmd, self.body.id)
            if text:
                text = text.decode().strip().replace(u"\u00a0", " ")

            if not text:
                self.statistics['no-text'] += 1
                file.textStatus = 'no-text'
                file.save()
            else:
                self.statistics['successful'] += 1
                file.text = text
                file.textStatus = 'successful'
                file.save()

            os.unlink(file_path)
