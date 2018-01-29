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
import math
import datetime
import subprocess
from ..models import *
from minio.error import ResponseError, NoSuchKey


class GenerateSitemap():
    def __init__(self, main):
        self.main = main

    def run(self, body_id, *args):
        if not self.main.config.ENABLE_PROCESSING:
            return
        self.body = Body.objects(uid=body_id).no_cache().first()
        if not self.body:
            return

        self.sitemaps = []
        self.tidy_up()
        self.generate_paper_sitemap()
        self.generate_meeting_sitemap()
        self.generate_file_sitemap()
        self.generate_meta_sitemap()
        # Create meta-sitemap

    def tidy_up(self):
        for sitemap_file in os.listdir(self.main.config.SITEMAP_DIR):
            if sitemap_file[0:24] == str(self.body.id):
                file_path = os.path.join(self.main.config.SITEMAP_DIR, sitemap_file)
                os.unlink(file_path)

    def generate_paper_sitemap(self):
        document_count = Paper.objects(body=self.body.id).count()
        for sitemap_number in range(0, int(math.ceil(document_count / 50000))):
            papers = Paper.objects(body=self.body.id)[sitemap_number * 50000:((sitemap_number + 1) * 50000) - 1]
            sitemap_path = os.path.join(self.main.config.SITEMAP_DIR, '%s-paper-%s.xml' % (self.body.id, sitemap_number))
            with open(sitemap_path, 'w') as f:
                f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
                f.write("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")
                for paper in papers.all():
                    f.write("  <url><loc>%s/paper/%s</loc><lastmod>%s</lastmod></url>\n" % (self.main.config.SITEMAP_BASE_URL, paper.id, paper.modified.strftime('%Y-%m-%d')))
                f.write("</urlset>\n")
            subprocess.call(['gzip', sitemap_path])
            self.sitemaps.append('%s-paper-%s.xml' % (self.body.id, sitemap_number))
            sitemap_number += 1


    def generate_file_sitemap(self):
        document_count = File.objects(body=self.body.id).count()
        for sitemap_number in range(0, int(math.ceil(document_count / 50000))):
            files = File.objects(body=self.body.id)[sitemap_number * 50000:((sitemap_number + 1) * 50000) - 1]
            sitemap_path = os.path.join(self.main.config.SITEMAP_DIR, '%s-file-%s.xml' % (self.body.id, sitemap_number))
            with open(sitemap_path, 'w') as f:
                f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
                f.write("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")
                for file in files.all():
                    f.write("  <url><loc>%s/file/%s</loc><lastmod>%s</lastmod></url>\n" % (self.main.config.SITEMAP_BASE_URL, file.id, file.modified.strftime('%Y-%m-%d')))
                f.write("</urlset>\n")
            subprocess.call(['gzip', sitemap_path])
            self.sitemaps.append('%s-file-%s.xml' % (self.body.id, sitemap_number))
            sitemap_number += 1

    def generate_meeting_sitemap(self):
        document_count = Meeting.objects(body=self.body.id).count()
        for sitemap_number in range(0, int(math.ceil(document_count / 50000))):
            meetings = Meeting.objects(body=self.body.id)[sitemap_number * 50000:((sitemap_number + 1) * 50000) - 1]
            sitemap_path = os.path.join(self.main.config.SITEMAP_DIR, '%s-meeting-%s.xml' % (self.body.id, sitemap_number))
            with open(sitemap_path, 'w') as f:
                f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
                f.write("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")
                for meeting in meetings.all():
                    if meeting.modified:
                        f.write("  <url><loc>%s/meeting/%s</loc><lastmod>%s</lastmod></url>\n" % (self.main.config.SITEMAP_BASE_URL, meeting.id, meeting.modified.strftime('%Y-%m-%d')))
                f.write("</urlset>\n")
            subprocess.call(['gzip', sitemap_path])
            self.sitemaps.append('%s-meeting-%s.xml' % (self.body.id, sitemap_number))
            sitemap_number += 1


    def generate_meta_sitemap(self):
        meta_sitemap_path = os.path.join(self.main.config.SITEMAP_DIR, '%s.xml' % self.body.id)
        with open(meta_sitemap_path, 'w') as f:
            f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            f.write("<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")
            for sitemap_name in self.sitemaps:
                f.write("  <sitemap><loc>%s/static/sitemap/%s.gz</loc></sitemap>\n" % (self.main.config.SITEMAP_BASE_URL, sitemap_name))
            f.write("</sitemapindex>\n")


