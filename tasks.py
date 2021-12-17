from celery import Celery
import boto3
from botocore.config import Config
import datetime
import json
from utils.s3_strorage_service import S3StorageService
from settings import AWS_ACCESS_KEY_ID,\
                        AWS_SECRET_ACCESS_KEY,\
                        AWS_STORAGE_BUCKET_NAME_AR_MESSAGES_SOURCE,\
                        AWS_STORAGE_BUCKET_NAME_AR_MESSAGES,\
                        AWS_QUEUE_NAME

ACCESS_KEY = AWS_ACCESS_KEY_ID
SECRET_KEY = AWS_SECRET_ACCESS_KEY

app = Celery('videomatting', broker='pyamqp://guest:guest@rabbit:5672/')


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(10.0, check_queue.s('testing...'), name='check incoming queue')


my_config = Config(
    region_name='us-east-1',
    signature_version='v4',
)

# Create SQS client
sqs = boto3.resource('sqs', 
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=my_config
    )
queue = sqs.get_queue_by_name(QueueName=AWS_QUEUE_NAME)

@app.task
def check_queue(arg):
    for message in queue.receive_messages(MessageAttributeNames=[]):
        try:
            data = json.loads(message.body)
            if 'web_app' == data['src']:
                print('DATA: ', data)
                id = data['ar_id']
                source_s3_key = data['s3_id_request']

                # we have the keys so ack the message else it may take some time
                message.delete()

                # download the original vid file for processing
                processing_log = f"{datetime.datetime.now()}: start\n"
                source_s3_storage = S3StorageService(AWS_STORAGE_BUCKET_NAME_AR_MESSAGES_SOURCE)

                src_s3_key = str(source_s3_key)
                source_s3_storage.download_file(src_s3_key, f"/tmp/{src_s3_key}")
                processing_log += f"{datetime.datetime.now()}: src file downloaded to /tmp/{src_s3_key}\n"

                # say that we've downloaded the file
                queue.send_message(
                        MessageBody=json.dumps({"src":"video_app", "ar_id":id, "progress":processing_log}),
                    )

                # P R O C E SS  HERE[START]
                # P R O C E SS  HERE[END]

                # upload the processed vid to s3
                processing_log = f"{datetime.datetime.now()}: start uploading to s3\n"
                s3_service = S3StorageService(AWS_STORAGE_BUCKET_NAME_AR_MESSAGES)
                with open(f"/tmp/{src_s3_key}", "rb") as file_obj: # processed__
                    s3_service.upload(file_obj, s3_key=f"processed__{src_s3_key}", file_name=file_obj.name, public=True)

                processing_log += f"{datetime.datetime.now()}: complete uploaded to s3\n"

                # say that we've downloaded the file
                queue.send_message(
                        MessageBody=json.dumps({"src":"video_app", "ar_id":id, "progress":processing_log}),
                    )

                # messages arrive at no particular order? so delay is finished
                import time
                time.sleep(5)
                
                queue.send_message(
                        MessageBody=json.dumps({"src":"video_app", "ar_id":id, "is_finished":1, 's3_key':f"processed__{src_s3_key}"}),
                    )
                print("SQS messages sent")
        except:
            print('Error parsing json or key error')
