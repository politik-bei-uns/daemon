from oparlsync.common.constants import BaseConfig


class DefaultConfig(BaseConfig):
    DEBUG = True

    ENABLE_PROCESSING = True

    S3_ACCESS_KEY = ''
    S3_SECRET_KEY = ''
    S3_PUBLIC_URL = ''

    ES_ENABLED = True
    ES_HOSTS = ['localhost']

#    BODY_LIST_MODE = 'whitelist'
#    BODY_LIST = ['DE-XXXX']
#    REGION_LIST_MODE = 'whitelist'
#    REGION_LIST = ['DE', 'DE-05', 'DE-XXXX']

    ADMINS = ['']
    MAIL_FROM = ''

    MAIL_HOST = ''
    MAIL_USERNAME = ''
    MAIL_PASSWORD = ''


class DevelopmentConfig(DefaultConfig):
    MODE = 'DEVELOPMENT'


class StagingConfig(DefaultConfig):
    MODE = 'STAGING'


class ProductionConfig(DefaultConfig):
    MODE = 'PRODUCTION'


def get_config(MODE):
    SWITCH = {
        'DEVELOPMENT': DevelopmentConfig,
        'STAGING': StagingConfig,
        'PRODUCTION': ProductionConfig
    }
    return SWITCH[MODE]
