
HOST = '127.0.0.1'
PORT = '2437'

# '<host>:<port>'
PEERS = []


LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '[%(levelname)s] %(asctime)s - %(module)s - %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    'handlers': {
        'console':{
            'level':'DEBUG',
            'class':'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'loggers': {
        'aioraft': {
            'handlers': ['console'],
            'level': 'DEBUG',
        }
    }
}
