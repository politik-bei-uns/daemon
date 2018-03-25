# encoding: utf-8

"""
Copyright (c) 2017, Ernesto Ruge
All rights reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import os
import sys
import errno
import signal
import daemon
import logging
import pymongo
import lockfile
import json
from copy import deepcopy
import daemon.pidfile
from multiprocessing import Value
from setproctitle import setproctitle
from .config import get_config
from .mongoqueue import MongoQueue

from .worker import Worker

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


class OparlSync():
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

    default_config = {
        "id": "",
        "rgs": "",
        "url": "",
        "force_full_sync": 0,
        "wait_time": 0.2,
        "geofabrik_package": False,
        "osm_relation": False,
        "name": False
    }

    def __init__(self):
        self.load_config()
        self.init_statuslog()

    def single(self, module, body_id, *args):
        if module not in self.modules:
            sys.exit('fatal: module does not exist.')
        current_module = self.modules[module](body_id)
        current_module.run(body_id, *args)

    def queue_add(self, module, body_id, *args):
        self.init_queue()
        if module not in self.modules:
            sys.exit('fatal: module should be one of %s' % '|'.join(self.modules.keys()))
        if body_id == 'all':
            bodies = os.listdir(self.config.BODY_DIR)
            for body in bodies:
                if body[-4:] == 'json':
                    body_config = self.get_body_config(filename=body)
                    if body_config['active'] and 'legacy' not in body_config:
                        payload = {
                            'module': module,
                            'body_id': body_config['id']
                        }
                        self.queue_network.put(payload)
        else:
            if not os.path.isfile(os.path.join(self.config.BODY_DIR, body_id + '.json')):
                sys.exit('fatal: body config does not exist')
            payload = {
                'module': module,
                'body_id': body_id
            }
            self.queue_network.put(payload)

    def queue_clear(self):
        self.init_queue()
        self.queue_network.clear_safe()

    def queue_list(self):
        self.init_queue()
        jobs = self.queue_network.next()

    def queue_details(self):
        self.init_queue()
        jobs = self.queue_network.details()
        print('| Body ID        | Job                    | Status    |')
        print('|----------------|------------------------|-----------|')
        for job in jobs:
            print('| %s | %s | %s |' % (job['body_id'], job['module'].ljust(22), job['status'].ljust(8)))

    def queue_stats(self):
        self.init_queue()
        stats = self.queue_network.stats()
        print('available: %s' % (int(stats['available'])))
        print('locked   : %s' % (int(stats['locked'])))
        print('total    : %s' % (int(stats['total'])))
        print('errors   : %s' % (int(stats['errors'])))

    def get_daemon_status(self):
        if not os.path.isfile(os.path.join(self.config.TMP_DIR, 'app.pid')):
            return False
        pidfile = open(os.path.join(self.config.TMP_DIR, 'app.pid'), 'r')
        pid = int(pidfile.readline().strip())
        if not pid:
            return False
        if pid < 0:
            return False
        try:
            os.kill(pid, 0)
        except OSError as err:
            if err.errno == errno.ESRCH:
                return False
            elif err.errno == errno.EPERM:
                return pid
            else:
                raise
        return pid

    def daemon_stop(self):
        status = self.get_daemon_status()
        if not status:
            sys.exit('Daemon is not running.')
        os.kill(status, signal.SIGTERM)
        self.statuslog.info('Stopping daemon ... (this may take some time!)')

    def daemon_status(self):
        status = self.get_daemon_status()
        if status == False:
            print('Daemon not running.')
        else:
            print('Daemon running with pid %s.' % status)

    def daemon_start(self, detach_process=True):
        if self.get_daemon_status():
            sys.exit('Daemon already running.')
        #stdout = open(os.path.join(self.config.LOG_DIR, 'output.log'), 'w+')
        #stderr = open(os.path.join(self.config.LOG_DIR, 'error.log'), 'w+')

        daemon_context = daemon.DaemonContext(
            working_directory=self.config.BASE_DIR,
            umask=0o002,
            pidfile=daemon.pidfile.PIDLockFile(os.path.join(self.config.TMP_DIR, 'app.pid')),
            detach_process=detach_process
            #stdout=stdout,
            #stderr=stderr
        )

        daemon_context.signal_map = {
            signal.SIGINT: self.program_cleanup,
            signal.SIGTERM: self.program_cleanup,
            signal.SIGHUP: 'terminate',
            signal.SIGUSR1: None
        }

        self.statuslog.info('Starting daemon ...')
        setproctitle('%s: master ' % (self.config.PROJECT_NAME))
        try:
            with daemon_context:
                self.init_queue()

                # Logging (Status)

                self.do_shutdown = Value('i', 0)

                # Thread-Liste (Netzwerk)
                self.threads_network = []
                for i in range(0, self.config.THREADS_NETWORK_MAX):
                    self.threads_network.append(Worker('network-%s' % (i + 1), do_shutdown=self.do_shutdown))
                    self.threads_network[i].start()

                for i in range(0, self.config.THREADS_NETWORK_MAX):
                    self.threads_network[i].join()

        except lockfile.AlreadyLocked:
            self.statuslog.info('Daemon already running.')


    def program_cleanup(self, signal, frame):
        self.statuslog.info("\nShutting down (this will take some time!) ...")
        self.do_shutdown.value = 1

    def load_config(self):
        self.config = get_config(os.getenv('APPLICATION_MODE', 'DEVELOPMENT'))()

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

    def init_statuslog(self):
        self.statuslog = logging.getLogger('statuslog')
        self.statuslog.setLevel(logging.DEBUG)
        statuslog_file_handler = logging.FileHandler("%s/main-status.log" % (self.config.LOG_DIR))
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

    def get_body_config(self, body_id=False, filename=False):
        if not filename:
            filename = '%s.json' % (body_id)
        try:
            with open('%s/%s' % (self.config.BODY_DIR, filename)) as body_config_file:
                if not body_config_file:
                    return False
                try:
                    body_config = deepcopy(self.default_config)
                    body_config.update(json.load(body_config_file))
                    return body_config
                except ValueError:
                    return False
        except FileNotFoundError:
            return False

