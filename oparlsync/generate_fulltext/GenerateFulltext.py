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
from minio.error import ResponseError, NoSuchKey


class GenerateFulltext():
    def __init__(self, main):
        self.main = main
        self.statistics = {
            'wrong-mimetype': 0,
            'file-missing': 0,
            'no-text': 0,
            'successful': 0
        }

    def run(self, body_id, *args):
        if not self.main.config.ENABLE_PROCESSING:
            return
        self.body_config = self.main.get_body_config(body_id)
        body_id = Body.objects(originalId=self.body_config['url']).first().id
        files = File.objects(textStatus__exists=False, body=body_id).timeout(False).all()
        for file in files:
            self.main.datalog.info('processing file %s' % file.id)
            file.modified = datetime.datetime.now()
            file.textGenerated = datetime.datetime.now()
            # get file
            try:
                data = self.main.s3.get_object(
                    self.main.config.S3_BUCKET,
                    "files/%s/%s" % (body_id, file.id)
                )
            except NoSuchKey:
                self.main.datalog.warn('file not found: %s' % file.id)
                self.statistics['file-missing'] += 1
                file.textStatus = 'file-missing'
                file.save()
                continue

            file_path = os.path.join(self.main.config.TMP_FULLTEXT_DIR, str(file.id))

            # save file
            with open(file_path, 'wb') as file_data:
                for d in data.stream(32 * 1024):
                    file_data.write(d)

            # decide app based on mimetype
            if file.mimeType == 'application/pdf':
                cmd = '%s -nopgbrk -enc UTF-8 %s -' % (self.main.config.PDFTOTEXT_COMMAND, file_path)
            elif file.mimeType == 'application/msword':
                cmd = '%s --to=txt --to-name=fd://1 %s' % (self.main.config.ABIWORD_COMMAND, file_path)
            else:
                cmd = None
                self.statistics['wrong-mimetype'] += 1
                file.textStatus = 'bad-mimetype'
                file.save()
                os.unlink(file_path)
                continue

            text = self.execute(cmd)
            if text is not None:
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

    def execute(self, cmd):
        new_env = os.environ.copy()
        new_env['XDG_RUNTIME_DIR'] = '/tmp/'
        output, error = subprocess.Popen(
            cmd.split(' '), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=new_env).communicate()
        if error is not None and error.decode().strip() != '' and 'WARNING **: clutter failed 0, get a life.' not in error.decode():
            self.main.datalog.debug("pdf output at command %s; output: %s" % (cmd, error.decode()))
        return output
