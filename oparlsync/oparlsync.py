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
import lockfile
import daemon.pidfile
import multiprocessing
from multiprocessing import Process, Value
from multiprocessing.managers import SyncManager

from .common import Common
from .worker import Worker


class OparlSync():
    def __init__(self):
        self.common = Common(prefix='main')

    def single(self, module, body_id, *args):
        self.common.update_datalog(module, body_id)
        self.common.job_type = 'interactive'
        if module not in self.common.modules:
            sys.exit('fatal: module does not exist.')
        current_module = self.common.modules[module](self.common)
        current_module.run(body_id, *args)

    def queue_add(self, module, body_id, *args):
        if module not in self.common.modules:
            sys.exit('fatal: module should be one of %s' % '|'.join(self.modules.keys()))
        if body_id == 'all':
            bodies = os.listdir(self.common.config.BODY_DIR)
            for body in bodies:
                if body[-4:] == 'json':
                    body_config = self.common.get_body_config(filename=body)
                    if body_config['active']:
                        payload = {
                            'module': module,
                            'body_id': body_config['id']
                        }
                        self.common.queue_network.put(payload)
        else:
            if not os.path.isfile(os.path.join(self.common.config.BODY_DIR, body_id + '.json')):
                sys.exit('fatal: body config does not exist')
            payload = {
                'module': module,
                'body_id': body_id
            }
            self.common.queue_network.put(payload)

    def queue_clear(self):
        self.common.queue_network.clear()

    def queue_list(self):
        jobs = self.common.queue_network.next()

    def queue_stats(self):
        stats = self.common.queue_network.stats()
        print('available: %s' % (int(stats['available'])))
        print('locked   : %s' % (int(stats['locked'])))
        print('total    : %s' % (int(stats['total'])))
        print('errors   : %s' % (int(stats['errors'])))

    def get_daemon_status(self):
        if not os.path.isfile(os.path.join(self.common.config.TMP_DIR, 'app.pid')):
            return False
        pidfile = open(os.path.join(self.common.config.TMP_DIR, 'app.pid'), 'r')
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
        self.common.statuslog.info('Stopping daemon ... (this may take some time!)')

    def daemon_status(self):
        status = self.get_daemon_status()
        if status == False:
            print('Daemon not running.')
        else:
            print('Daemon running with pid %s.' % status)

    def daemon_start(self):
        if self.get_daemon_status():
            sys.exit('Daemon already running.')
        stdout = open(os.path.join(self.common.config.LOG_DIR, 'output.log'), 'w+')
        stderr = open(os.path.join(self.common.config.LOG_DIR, 'error.log'), 'w+')

        daemon_context = daemon.DaemonContext(
            working_directory=self.common.config.BASE_DIR,
            umask=0o002,
            pidfile=daemon.pidfile.PIDLockFile(os.path.join(self.common.config.TMP_DIR, 'app.pid'))#,
            #stdout=stdout,
            #stderr=stderr
        )

        daemon_context.signal_map = {
            signal.SIGTERM: self.program_cleanup,
            signal.SIGHUP: 'terminate',
            signal.SIGUSR1: None
        }

        self.common.statuslog.info('Starting daemon ...')
        try:
            with daemon_context:
                # Logging (Status)

                self.do_shutdown = Value('i', 0)

                # Thread-Liste (Netzwerk)
                self.threads_network = []
                for i in range(0, self.common.config.THREADS_NETWORK_MAX):
                    self.threads_network.append(Worker('network-%s' % (i + 1), do_shutdown=self.do_shutdown))
                    self.threads_network[i].start()

                for i in range(0, self.common.config.THREADS_NETWORK_MAX):
                    self.threads_network[i].join()
        except lockfile.AlreadyLocked:
            self.common.statuslog.info('Daemon already running.')

    def program_cleanup(self, signal, frame):
        self.common.statuslog.info("\nShutting down (this will take some time!) ...")
        self.do_shutdown.value = 1
        """
        # Thread-Liste (Lokal)
        self.threads_local = []
        for i in range(0, self.config.THREADS_LOCAL_MAX):
          self.threads_local.append(Worker(self, i + 1))
          self.threads_local[i].start()
        self.tick = 0
        """
        """
        # Primäre Loop
        try:
          while True:
            if self.tick % 10 == 0:
              #self.statuslog.debug('Check for new jobs ...')
              network_busy_before = 0
              network_busy_after = 0
              for i in range(0, self.config.THREADS_NETWORK_MAX):
                if self.threads_network[i].get_status():
                  network_busy_before += 1
                  network_busy_after += 1
                else:
                  #self.statuslog.debug('Give a job to network thread %s' % (i + 1))
                  new_job = self.queue_network.next()
                  if new_job:
                    self.threads_network[i].set_job(new_job)
                    network_busy_after += 1
            if self.tick >= 100000:
              self.tick = 0
            self.tick += 1
            time.sleep(.1)
        except KeyboardInterrupt:
          self.statuslog.info("\nShutting down (this will take some time!)...")
          self.run_event.clear()
          for thread in self.threads_network:
            thread.join()
          for thread in self.threads_local:
            thread.join()
          self.statuslog.info("Successfully shut down.")
        """
