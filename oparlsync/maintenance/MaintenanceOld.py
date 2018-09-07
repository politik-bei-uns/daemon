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
import requests
import threading
import hashlib
from slugify import slugify
from ..models import *
from pymongo import MongoClient


class MaintenanceOld:

    def old_import(self):
        client = MongoClient()
        db = client.ris
        """
        for body_raw in db.body.find():
            body = Body()
            body.id = body_raw['_id']
            print('save body %s' % body.id)
            body.legacy = True
            body.rgs = body_raw['regionalschlüssel']
            body.name = body_raw['name']
            body.created = body_raw['created']
            body.modified = body_raw['modified']
            body.save()


        for paper_raw in db.paper.find(no_cursor_timeout=True):
            if 'body' not in paper_raw:
                continue
            paper = Paper()
            paper.id = paper_raw['_id']
            paper.legacy = True
            print('save paper %s' % paper.id)

            paper.body = paper_raw['body'].id

            if 'name' in paper_raw:
                paper.name = paper_raw['name']
            elif 'title' in paper_raw:
                paper.name = paper_raw['title']

            if 'reference' in paper_raw:
                paper.reference = paper_raw['reference']
            elif 'nameShort' in paper_raw:
                paper.reference = paper_raw['nameShort']

            if 'publishedDate' in paper_raw:
                paper.date = paper_raw['publishedDate']

            if 'paperType' in paper_raw:
                paper.paperType = paper_raw['paperType']
            if 'created' in paper_raw:
                paper.created = paper_raw['created']
            if 'modified' in paper_raw:
                paper.modified = paper_raw['modified']

            if 'mainFile' in paper_raw:
                file = File()
                file.id = paper_raw['mainFile'].id
                file.save()
                paper.mainFile = file.id
            if 'auxiliaryFile' in paper_raw:
                paper.auxiliaryFile = []
                for file_raw in paper_raw['auxiliaryFile']:
                    file = File()
                    file.id = file_raw.id
                    file.save()
                    paper.auxiliaryFile.append(file.id)

            paper.save()

        """
        for file_raw in db.file.find(no_cursor_timeout=True):
            if 'body' not in file_raw or 'mimetype' not in file_raw:
                continue
            file = File()
            file.id = file_raw['_id']
            file.legacy = True
            file.mimeType = file_raw['mimetype']
            file.body = file_raw['body'].id
            print('save file %s from body %s' % (file.id, file.body))
            if 'filename' in file_raw:
                file.fileName = file_raw['filename']
            if 'name' in file_raw:
                file.fileName = file_raw['name']
            if 'created' in file_raw:
                file.created = file_raw['created']
            if 'modified' in file_raw:
                file.modified = file_raw['modified']
            if 'filename' in file_raw:
                file.fileName = file_raw['filename']

            r = requests.get('https://politik-bei-uns.de/file/%s/download' % file.id, stream=True)

            if r.status_code != 200:
                file.downloaded = False
            else:
                file_path = os.path.join(self.config.TMP_OLD_IMPORT_DIR, str(file.id))
                with open(file_path, 'wb') as file_data:
                    for chunk in r.iter_content(chunk_size=32 * 1024):
                        if chunk:
                            file_data.write(chunk)
                file.size = os.path.getsize(file_path)
                with open(file_path, 'rb') as checksum_file:
                    checksum_file_content = checksum_file.read()
                    file.sha1Checksum = hashlib.sha1(checksum_file_content).hexdigest()

                metadata = {
                    'Content-Disposition': 'filename=%s' % file.fileName if file.fileName else str(file.id)
                }
                self.s3.fput_object(
                    self.config.S3_BUCKET,
                    "files/%s/%s" % (file.body, file.id),
                    file_path,
                    content_type=file.mimeType,
                    metadata= metadata
                )
                file.downloaded = True
                os.remove(file_path)
            #file.save()
            print('thread count: %s' % threading.active_count())

    def fix_nameless_files(self, body=False):
        mimetypes = {
            "application/pdf": 'pdf',
            "image/jpeg": 'jpg',
            "image/gif": 'gif',
            "application/zip": 'zip',
            "image/tiff": 'tiff',
            "image/x-ms-bmp": 'bmp',
            "application/msword": 'doc',
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": 'docx',
            "text/html": 'html',
            "application/vnd.ms-excel": 'xls',
            "text/rtf": 'rtf',
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": 'xlsx',
            "text/plain": 'txt',
            "application/vnd.ms-powerpoint": 'ppt'
        }

        files = File.objects(legacy=True).no_cache().timeout(False).all()
        for file in files:
            if not file.name and file.downloaded:
                file_path = os.path.join(self.config.TMP_FILE_DIR, str(file.id))
                self.get_file(file, file_path)
                self.s3.remove_object(
                    self.config.S3_BUCKET,
                    "files/%s/%s" % (file.body.id, file.id),
                )
                if file.fileName:
                    file_name = file.fileName
                else:
                    file_name = str(file.id)
                    if file.paper:
                        if len(file.paper):
                            if file.paper[0].name:
                                file_name = slugify(file.paper[0].name)
                    if file.mimeType in mimetypes:
                        file_name += '.' + mimetypes[file.mimeType]
                metadata = {
                    'Content-Disposition': 'filename=%s' % file_name
                }
                print('fix file id %s with file name %s' % (file.id, file_name))
                self.s3.fput_object(
                    self.config.S3_BUCKET,
                    "files/%s/%s" % (file.body.id, file.id),
                    file_path,
                    content_type=file.mimeType,
                    metadata=metadata
                )

    def correct_regions(self):
        for body in Body.objects.no_cache().all():
            if body.region:
                for street in Street.objects(body=body).no_cache().all():
                    street.region = body.region
                    street.save()
                for street_number in StreetNumber.objects(body=body).no_cache().all():
                    street_number.region = body.region
                    street_number.save()
