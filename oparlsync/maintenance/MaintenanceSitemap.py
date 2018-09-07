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
from ..models import *


class MaintenanceSitemap:
    def sitemap_master(self):
        meta_sitemap_path = os.path.join(self.config.SITEMAP_DIR, 'sitemap.xml')
        with open(meta_sitemap_path, 'w') as f:
            f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            f.write("<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")
            for body in Body.objects.all():
                if not body.legacy:
                    f.write("  <sitemap><loc>%s/static/sitemap/%s-meeting-0.xml.gz</loc></sitemap>\n" % (self.config.SITEMAP_BASE_URL, body.id))
                f.write("  <sitemap><loc>%s/static/sitemap/%s-paper-0.xml.gz</loc></sitemap>\n" % (self.config.SITEMAP_BASE_URL, body.id))
                f.write("  <sitemap><loc>%s/static/sitemap/%s-file-0.xml.gz</loc></sitemap>\n" % (self.config.SITEMAP_BASE_URL, body.id))
                if File.objects(body=body.id, deleted__ne=True).count() > 50000:
                    f.write("  <sitemap><loc>%s/static/sitemap/%s-file-1.xml.gz</loc></sitemap>\n" % (self.config.SITEMAP_BASE_URL, body.id))
            f.write("</sitemapindex>\n")


