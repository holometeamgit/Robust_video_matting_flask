AWS_ACCESS_KEY_ID = 'AKIA2VJEQIYYTAXFMGWN'
AWS_SECRET_ACCESS_KEY = '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'

AWS_S3_ENDPOINT_URL = None

AWS_STORAGE_BUCKET_NAME_AR_MESSAGES_SOURCE = 'dev.ar-messages-source'
AWS_STORAGE_BUCKET_NAME_AR_MESSAGES = 'dev.ar-messages-processed'

if True:
    AWS_QUEUE_NAME = 'prod-armessage-std'

    GPU_ENABLED = True
    CELERY_BROKER = 'pyamqp://guest:guest@127.0.0.1:5672/'

    CONFIG_REGION = 'eu-west-2'
else:
    AWS_QUEUE_NAME = 'g_dev_local'

    GPU_ENABLED = False
    CELERY_BROKER = 'pyamqp://guest:guest@rabbit:5672/'

    CONFIG_REGION = 'us-east-1'