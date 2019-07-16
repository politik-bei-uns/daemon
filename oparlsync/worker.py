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
import time
import signal
import pymongo
import logging
import smtplib
import traceback
from multiprocessing import Process
from setproctitle import setproctitle
from .config import get_config
from .mongoqueue import MongoQueue

from oparlsync.oparl_download import OparlDownload
from oparlsync.generate_thumbnails import GenerateThumbnails
from oparlsync.generate_fulltext import GenerateFulltext
from oparlsync.maintenance import Maintenance
from oparlsync.street_import import StreetImport
from oparlsync.generate_georeferences import GenerateGeoreferences
from oparlsync.generate_backrefs import GenerateBackrefs
from oparlsync.elasticsearch_import import ElasticsearchImport
from oparlsync.generate_sitemap import GenerateSitemap
from oparlsync.misc import Misc



class Worker(Process):
    modules = {
        'oparl_download': OparlDownload,
        'generate_thumbnails': GenerateThumbnails,
        'generate_fulltext': GenerateFulltext,
        'maintenance': Maintenance,
        'street_import': StreetImport,
        'generate_georeferences': GenerateGeoreferences,
        'generate_backrefs': GenerateBackrefs,
        'elasticsearch_import': ElasticsearchImport,
        'generate_sitemap': GenerateSitemap,
        'misc': Misc
    }

    def __init__(self, process_name, do_shutdown, **kwargs):
        super(Worker, self).__init__()
        self.process_name = process_name
        self.do_shutdown = do_shutdown
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def run(self):
        self.tick = 0
        self.load_config()
        self.init_statuslog()
        self.init_queue()

        if self.config.ENABLE_PROCESSING:
            self.next_job = {
                'oparl_download': ['generate_backrefs', 'generate_thumbnails'],
                'generate_backrefs': ['generate_fulltext'],
                'generate_fulltext': ['generate_georeferences'],
                'generate_georeferences': ['elasticsearch_import'],
                'elasticsearch_import': ['misc', 'generate_sitemap']
            }
        else:
            self.next_job = {
                'oparl_download': ['generate_backrefs']
            }
        #self.common = Common(prefix=self.process_name)
        setproctitle('%s worker: idle ' % (self.config.PROJECT_NAME))
        self.statuslog.info('Process %s started!' % self.process_name)
        while True:
            if self.tick % 100 == 0:
                job = self.queue_network.next()

                if job:
                    current_module = None
                    try:
                        setproctitle('%s worker: %s %s ' % (self.config.PROJECT_NAME, job.payload['module'], job.payload['body_id']))
                        #self.common.update_datalog(job.payload['module'], job.payload['body_id'])
                        current_module = self.modules[job.payload['module']](job.payload['body_id'])
                        current_module.run(job.payload['body_id'])
                    except:
                        self.send_mail(
                            self.config.ADMINS,
                            'critical error at oparl-mirror',
                            "Body ID: %s\nBacktrace:\n%s" % (job.payload['body_id'], traceback.format_exc())
                        )
                    finally:
                        if current_module:
                            current_module.close()
                        self.add_next_to_queue(job.payload['module'], job.payload['body_id'])
                        job.complete()
                        current_module = None
                        setproctitle('%s worker: idle ' % (self.config.PROJECT_NAME))
            if self.tick >= 100000:
                self.tick = 0
            self.tick += 1
            time.sleep(.1)
            if self.do_shutdown.value == 1:
                self.graceful_shutdown()
                break

    def graceful_shutdown(self):
        print("Shutdown of %s complete!" % self.process_name)

    def init_queue(self):
        try:
            db_raw_client = pymongo.MongoClient(
                host=self.config.MONGO_DB_HOST,
                port=self.config.MONGO_DB_PORT,
                connect=False
            )
            db_raw_client.server_info()
        except pymongo.errors.ServerSelectionTimeoutError as err:
            sys.exit('fatal: connection to MongoDB can\'t be established.')
        db_raw = db_raw_client[self.config.MONGO_DB_NAME]
        # Queue (Netzwerk)
        self.queue_network = MongoQueue(
            db_raw.queue_network,
            consumer_id="main",
            timeout=300,
            max_attempts=3
        )

    def add_next_to_queue(self, current_module, body_id):
        if current_module in self.next_job.keys():
            for next_module in self.next_job[current_module]:
                payload = {
                    'module': next_module,
                    'body_id': body_id
                }
                self.queue_network.put(payload)

    def load_config(self):
        self.config = get_config(os.getenv('APPLICATION_MODE', 'DEVELOPMENT'))()

    def init_statuslog(self):
        self.statuslog = logging.getLogger('statuslog')
        self.statuslog.setLevel(logging.DEBUG)
        statuslog_file_handler = logging.FileHandler("%s/%s-status.log" % (self.config.LOG_DIR, self.process_name))
        statuslog_file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s ')
        )
        statuslog_file_handler.setLevel(logging.INFO)
        self.statuslog.addHandler(statuslog_file_handler)

        statuslog_stream_handler = logging.StreamHandler()
        statuslog_stream_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s ')
        )
        statuslog_stream_handler.setLevel(logging.DEBUG)
        self.statuslog.addHandler(statuslog_stream_handler)

    def send_mail(self, receivers=None, subject='', body=''):
        smtp = smtplib.SMTP(
            host=self.config.MAIL_HOST
        )
        smtp.ehlo_or_helo_if_needed()
        if self.config.MAIL_USE_SSL:
            smtp.starttls()
            smtp.ehlo_or_helo_if_needed()
        smtp.login(
            user=self.config.MAIL_USERNAME,
            password=self.config.MAIL_PASSWORD
        )
        body_full = '\r\n'.join([
            'To: %s' % ', '.join(receivers),
            'From: %s' % self.config.MAIL_FROM,
            'Subject: %s' % subject,
            '', body
        ])
        smtp.sendmail(
            self.config.MAIL_FROM,
            receivers,
            body_full
        )
