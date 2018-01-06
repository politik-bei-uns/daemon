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


class DefaultConfig(object):
    PROJECT_NAME = "oparlsync"
    PROJECT_VERSION = '0.0.1'

    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    LOG_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir, 'logs'))
    BODY_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir, 'bodies'))
    REGION_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir, 'regions'))
    TMP_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir, 'tmp'))
    TMP_FILE_DIR = os.path.abspath(os.path.join(TMP_DIR, 'files'))
    TMP_FULLTEXT_DIR = os.path.abspath(os.path.join(TMP_DIR, 'fulltext'))
    TMP_THUMBNAIL_DIR = os.path.abspath(os.path.join(TMP_DIR, 'thumbnails'))
    TMP_OSM_DIR = os.path.abspath(os.path.join(TMP_DIR, 'osm'))
    TMP_REGION_DIR = os.path.abspath(os.path.join(TMP_DIR, 'region'))

    USE_MIRROR = False
    OPARL_MIRROR_PREFIX = ''
    OPARL_MIRROR_URL = ''

    DEBUG = True

    MONGO_DB_HOST = 'localhost'
    MONGO_DB_PORT = 27017
    MONGO_DB_NAME = 'oparl'

    THREADS_NETWORK_MAX = 4
    THREADS_LOCAL_MAX = 4
    GET_URL_WAIT_TIME = 0.2
    ENABLE_PROCESSING = True

    S3_ENDPOINT = '127.0.0.1:9000'
    S3_ACCESS_KEY = ''
    S3_SECRET_KEY = ''
    S3_SECURE = False
    S3_BUCKET = ''
    S3_LOCATION = 'us-east-1'
    S3_PUBLIC_URL = ''

    ES_ENABLED = True
    ES_HOSTS = []

    ADMINS = []
    MAIL_FROM = ''

    MAIL_HOST = ''
    MAIL_USE_SSL = True
    MAIL_USERNAME = ''
    MAIL_PASSWORD = ''

    SUBPROCESS_TIMEOUT = 600
    PDFTOTEXT_COMMAND = '/usr/bin/pdftotext'
    ABIWORD_COMMAND = '/usr/bin/abiword'
    GHOSTSCRIPT_COMMAND = '/usr/bin/gs'
    JPEGOPTIM_PATH = '/usr/bin/jpegoptim'
    OSMOSIS_PATH = os.path.join(BASE_DIR, 'street_import', 'osmosis', 'bin', 'osmosis')
    REL2POLY_PATH = os.path.join(BASE_DIR, 'street_import', 'rel2poly.pl')

    THUMBNAIL_SIZES = [1200, 800, 300, 150]


class DevelopmentConfig(DefaultConfig):
    pass


class StagingConfig(DefaultConfig):
    pass


class ProductionConfig(DefaultConfig):
    pass


def get_config(MODE):
    SWITCH = {
        'DEVELOPMENT': DevelopmentConfig,
        'STAGING': StagingConfig,
        'PRODUCTION': ProductionConfig
    }
    return SWITCH[MODE]
