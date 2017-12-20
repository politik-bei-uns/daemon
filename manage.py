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

import sys
from oparlsync import OparlSync

oparlsync = OparlSync()

args = sys.argv

global_action = ['daemon', 'single', 'queue']

if len(args) < 2:
  sys.exit('usage: python manage.py %s' % '|'.join(global_action))

if args[1] not in global_action:
  sys.exit('fatal: action should be one of %s' % '|'.join(global_action))

daemon_action = ['start', 'stop', 'status']

if args[1] == 'daemon':
  if len(args) < 3:
    sys.exit('usage: puthon manage.py queue %s' % '|'.join(daemon_action))
  
  if args[2] not in daemon_action:
    sys.exit('fatal: action should be one of %s' % '|'.join(daemon_action))
  
  if args[2] == 'start':
    oparlsync.daemon_start()
  
  if args[2] == 'stop':
    oparlsync.daemon_stop()
    
  if args[2] == 'status':
    oparlsync.daemon_status()

queue_action = ['add', 'clear', 'list', 'stats']

if args[1] == 'queue':
  if len(args) < 3:
    sys.exit('usage: puthon manage.py queue %s' % '|'.join(queue_action))
  
  if args[2] not in queue_action:
    sys.exit('fatal: action should be one of %s' % '|'.join(queue_action))
  
  if args[2] == 'add':
    if len(args) < 5:
      sys.exit('usage: python manage.py queue add $module $body')
    
    oparlsync.queue_add(args[3], args[4])
  
  if args[2] == 'clear':
    oparlsync.queue_clear()
  
  if args[2] == 'list':
    oparlsync.queue_list()
  
  if args[2] == 'stats':
    oparlsync.queue_stats()

if args[1] == 'single':
  if len(args) < 4:
    sys.exit('usage: python manage.py single $module $body $mongoid|$oparlid(optional)')
  
  oparlsync.single(args[2], args[3], *args[4:])

