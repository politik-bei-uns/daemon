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

import random
import time
import signal
import traceback
from multiprocessing import Process

from .common import Common
from .oparl_download import OparlDownload
from .generate_thumbnails import GenerateThumbnails


class Worker(Process):
    def __init__(self, process_name, do_shutdown, **kwargs):
        super(Worker, self).__init__()
        self.process_name = process_name
        self.do_shutdown = do_shutdown
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def run(self):
        self.tick = 0
        self.common = Common(prefix=self.process_name)
        self.common.statuslog.info('Process %s started!' % self.process_name)
        while True:
            if self.tick % 100 == 0:
                job = self.common.queue_network.next()
                if job:
                    try:
                        self.common.update_datalog(job.payload['module'], job.payload['body_id'])
                        current_module = self.common.modules[job.payload['module']](self.common)
                        current_module.run(job.payload['body_id'])
                    except:
                        self.common.send_mail(
                            self.common.config.ADMINS,
                            'critical error at oparl-mirror',
                            "Body ID: %s\nBacktrace:\n%s" % (job.payload['body_id'], traceback.format_exc())
                        )
                    finally:
                        self.common.add_next_to_queue(job.payload['module'], job.payload['body_id'])
                        job.complete()
            if self.tick >= 100000:
                self.tick = 0
            self.tick += 1
            time.sleep(.1)
            if self.do_shutdown.value == 1:
                self.graceful_shutdown()
                break

    def graceful_shutdown(self):
        print("Shutdown of %s complete!" % self.process_name)
