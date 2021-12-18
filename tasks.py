import datetime
import json
import logging

from celery import Celery
import boto3
from botocore.config import Config

from utils.s3_strorage_service import S3StorageService
from inference_yield import convert_video
from settings import AWS_ACCESS_KEY_ID,\
                        AWS_SECRET_ACCESS_KEY,\
                        AWS_STORAGE_BUCKET_NAME_AR_MESSAGES_SOURCE,\
                        AWS_STORAGE_BUCKET_NAME_AR_MESSAGES,\
                        AWS_QUEUE_NAME

ACCESS_KEY = AWS_ACCESS_KEY_ID
SECRET_KEY = AWS_SECRET_ACCESS_KEY

app = Celery('videomatting', broker='pyamqp://guest:guest@rabbit:5672/')
logger = logging.getLogger(__name__)

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
    for message in queue.receive_messages():
        logger.debug(f'Message received: {message.body}')
        try:
            data = json.loads(message.body)
            if 'web_app' == data['src']:
                logger.debug(f' ===> DATA from WebApp ===> : {data}')
                id = data['ar_id']
                source_s3_key = data['s3_id_request']
                logger.info(f'Request to remove background for Objects with id: {id}, fileo object: {source_s3_key}')

                # we have the keys so ack the message else it may take some time
                message.delete()

                # download the original vid file for processing
                processing_log = f"{datetime.datetime.now()}: start\n"
                source_s3_storage = S3StorageService(AWS_STORAGE_BUCKET_NAME_AR_MESSAGES_SOURCE)

                src_s3_key = str(source_s3_key)
                logger.info(f'Download {id}, file object: {src_s3_key} to /tmp/{src_s3_key}')
                source_s3_storage.download_file(src_s3_key, f"/tmp/{src_s3_key}")
                processing_log += f"{datetime.datetime.now()}: src file downloaded to /tmp/{src_s3_key}\n"

                # say that we've downloaded the file
                queue.send_message(
                        MessageBody=json.dumps({"src":"video_app", "ar_id":id, "progress":processing_log}),
                    )

                # P R O C E SS  HERE[START]
                try:
                    logger.info(f'Start conversion for Object {id}, file object: {src_s3_key}')
                    generator = convert_video(input_source=f'/tmp/{src_s3_key}',      # input file path WITH file name
                                output_dir='/tmp/',        # output file path WITHOUT file name
                                output_type='video',              # Choose only "video"
                                output_composition=f'processed__{src_s3_key}',  # output file name
                                output_video_mbps=4,              # Output video mbps.
                                downsample_ratio=None,            # A hyperparameter to adjust or use None for auto.
                                seq_chunk=12,                     # Frames chunk
                                server_uri='https://beem.me',        # current server uri, required for full output link
                                generate_seg_video=True)
                    for i, total in generator:
                        if i % 50 == 0:
                            processing_log = f"{datetime.datetime.now()}: {str(i)}/{total}\n"
                            logger.info(f'{processing_log} for Object {id}, file object: {src_s3_key}')
                            queue.send_message(
                                MessageBody=json.dumps({"src":"video_app", "ar_id":id, "progress":processing_log}),
                            )

                    logger.info(f'End conversion for Object {id}, file object: {src_s3_key}')
                except Exception as e:
                    logger.error(f'Could not process Object {id}, file object: {src_s3_key}')
                    logger.error(e)
                    processing_log = f"{datetime.datetime.now()}: Error processing \n"
                    queue.send_message(
                        MessageBody=json.dumps({"src":"video_app", "ar_id":id, "progress":processing_log}),
                    )
                    return
                # P R O C E SS  HERE[END]

                # upload the processed vid to s3
                logger.info(f'Upload {id}, file object from: /tmp/processed__{src_s3_key} to processed__{src_s3_key} s3 ')
                processing_log = f"{datetime.datetime.now()}: start uploading to s3\n"
                s3_service = S3StorageService(AWS_STORAGE_BUCKET_NAME_AR_MESSAGES)
                with open(f"/tmp/processed__{src_s3_key}", "rb") as file_obj: # processed__
                    s3_service.upload(file_obj, s3_key=f"processed__{src_s3_key}", file_name=file_obj.name, public=True)

                processing_log += f"{datetime.datetime.now()}: complete uploaded to s3\n"

                # messages arrive at no particular order? so delay is finished
                import time
                time.sleep(5)
                
                queue.send_message(
                        MessageBody=json.dumps({"src":"video_app", "ar_id":id, "is_finished":1, 's3_key':f"processed__{src_s3_key}"}),
                    )
                print("SQS messages sent")
        except:
            print('Error parsing json or key error')
