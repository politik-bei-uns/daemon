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
import sys
import shutil
import datetime
import subprocess
from PIL import Image
from ..models import Body, File
from minio.error import ResponseError, NoSuchKey


class GenerateThumbnails():
    def __init__(self, main):
        self.main = main
        self.statistics = {
            'wrong-mimetype': 0,
            'file-missing': 0,
            'successful': 0
        }

    def run(self, body_id, *args):
        if not self.main.config.ENABLE_PROCESSING:
            return
        self.body_config = self.main.get_body_config(body_id)
        body_id = Body.objects(originalId=self.body_config['url']).first().id
        files = File.objects(thumbnailStatus__exists=False, body=body_id).timeout(False).all()
        for file in files:
            self.main.datalog.info('processing file %s' % file.id)
            file.modified = datetime.datetime.now()
            file.thumbnailGenerated = datetime.datetime.now()
            # get file
            try:
                data = self.main.s3.get_object(
                    self.main.config.S3_BUCKET,
                    "files/%s/%s" % (body_id, file.id)
                )
            except NoSuchKey:
                self.main.datalog.warn('file not found: %s' % file.id)
                self.statistics['file-missing'] += 1
                file.thumbnailStatus = 'file-missing'
                file.thumbnailsGenerated = datetime.datetime.now()
                file.modified = datetime.datetime.now()
                file.save()
                continue

            file_path = os.path.join(self.main.config.TMP_THUMBNAIL_DIR, str(file.id))

            # save file
            with open(file_path, 'wb') as file_data:
                for d in data.stream(32 * 1024):
                    file_data.write(d)

            if file.mimeType not in ['application/msword', 'application/pdf']:
                self.main.datalog.warn('wrong mimetype: %s' % file.id)
                self.statistics['wrong-mimetype'] += 1
                file.thumbnailStatus = 'wrong-mimetype'
                file.thumbnailsGenerated = datetime.datetime.now()
                file.modified = datetime.datetime.now()
                file.save()
                os.unlink(file_path)
                continue

            if file.mimeType == 'application/msword':
                file_path_old = file_path
                file_path = file_path + '-old'
                cmd = ('%s --to=PDF -o %s %s' % (self.main.config.ABIWORD_COMMAND, file_path, file_path_old))
                self.execute(cmd)

            # create folders
            max_folder = os.path.join(self.main.config.TMP_THUMBNAIL_DIR, str(file.id) + '-max')
            if not os.path.exists(max_folder):
                os.makedirs(max_folder)
            out_folder = os.path.join(self.main.config.TMP_THUMBNAIL_DIR, str(file.id) + '-out')
            if not os.path.exists(out_folder):
                os.makedirs(out_folder)
            for size in self.main.config.THUMBNAIL_SIZES:
                if not os.path.exists(os.path.join(out_folder, str(size))):
                    os.makedirs(os.path.join(out_folder, str(size)))
            file.thumbnail = {}
            pages = 0

            # generate max images
            max_path = max_folder + os.sep + '%d.png'
            cmd = '%s -dQUIET -dSAFER -dBATCH -dNOPAUSE -sDisplayHandle=0 -sDEVICE=png16m -r100 -dTextAlphaBits=4 -sOutputFile=%s -f %s' % (
            self.main.config.GHOSTSCRIPT_COMMAND, max_path, file_path)
            self.execute(cmd)

            # generate thumbnails based on max images
            for max_file in os.listdir(max_folder):
                pages += 1
                file_path = os.path.join(max_folder, max_file)
                num = max_file.split('.')[0]
                im = Image.open(file_path)
                im = self.conditional_to_greyscale(im)
                (owidth, oheight) = im.size
                file.thumbnail[str(num)] = {
                    'page': int(num),
                    'pages': {}
                }

                for size in self.main.config.THUMBNAIL_SIZES:
                    (width, height) = self.scale_width_height(size, owidth, oheight)
                    # Two-way resizing
                    resizedim = im
                    if oheight > (height * 2.5):
                        # generate intermediate image with double size
                        resizedim = resizedim.resize((width * 2, height * 2), Image.NEAREST)
                    resizedim = resizedim.resize((width, height), Image.ANTIALIAS)
                    out_path = os.path.join(out_folder, str(size), str(num) + '.jpg')
                    resizedim.save(out_path, subsampling=0, quality=80)
                    # optimize image
                    cmd = '%s --preserve-perms %s' % (self.main.config.JPEGOPTIM_PATH, out_path)
                    self.execute(cmd)
                    # create mongodb object and append it to file
                    file.thumbnail[str(num)]['pages'][str(size)] = {
                        'width': width,
                        'height': height,
                        'filesize': os.path.getsize(out_path)
                    }
            # save all generated files in minio
            for size in self.main.config.THUMBNAIL_SIZES:
                for out_file in os.listdir(os.path.join(out_folder, str(size))):
                    try:
                        self.main.s3.fput_object(
                            self.main.config.S3_BUCKET,
                            "file-thumbnails/%s/%s/%s/%s" % (body_id, str(file.id), str(size), out_file),
                            os.path.join(out_folder, str(size), out_file),
                            'image/jpeg'
                        )
                    except ResponseError as err:
                        self.main.datalog.error(
                            'Critical error saving file from File %s from Body %s' % (file.id, self.body_uid))
            # save in mongodb
            file.thumbnailStatus = 'successful'
            file.thumbnailsGenerated = datetime.datetime.now()
            file.modified = datetime.datetime.now()
            file.pages = pages
            file.save()
            # tidy up
            os.unlink(file_path)
            shutil.rmtree(max_folder)
            shutil.rmtree(out_folder)

    def conditional_to_greyscale(self, image):
        """
        Convert the image to greyscale if the image information
        is greyscale only
        """
        bands = image.getbands()
        if len(bands) >= 3:
            # histogram for all bands concatenated
            hist = image.histogram()
            if len(hist) >= 768:
                hist1 = hist[0:256]
                hist2 = hist[256:512]
                hist3 = hist[512:768]
                # print "length of histograms: %d %d %d" % (len(hist1), len(hist2), len(hist3))
                if hist1 == hist2 == hist3:
                    # print "All histograms are the same!"
                    return image.convert('L')
        return image

    def scale_width_height(self, height, original_width, original_height):
        factor = float(height) / float(original_height)
        width = int(round(factor * original_width))
        return (width, height)

    def execute(self, cmd):
        new_env = os.environ.copy()
        new_env['XDG_RUNTIME_DIR'] = '/tmp/'
        output, error = subprocess.Popen(
            cmd.split(' '), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=new_env).communicate()
        try:
            if error is not None and error.decode().strip() != '' and 'WARNING **: clutter failed 0, get a life.' not in error.decode():
                self.main.datalog.debug("pdf output at command %s; output: %s" % (cmd, error.decode()))
        except UnicodeDecodeError:
            self.main.datalog.debug("pdf output at command %s; output: %s" % (cmd, error))
        return output