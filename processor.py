import datetime
from utils.s3_strorage_service import S3StorageService

from inference_yield import convert_video

def process_video(s3_object_key):
    src_s3_key = str(s3_object_key)

    processing_log = f"{datetime.datetime.now()}: download start\n"
    source_file = download_file_with_s3_key(src_s3_key, 'AWS_BUCKET_NAME')
    processing_log += f"{datetime.datetime.now()}: src file downloaded to /tmp/{src_s3_key}\n"
    processing_log += f"{datetime.datetime.now()}: downloaded complete \n"
    # SEND TO QUEUE

    try:
        # run bg matting process
        generator = convert_video(input_source=source_file,      # input file path WITH file name
                                output_dir="/tmp",        # output file path WITHOUT file name
                                output_type='video',              # Choose only "video"
                                output_composition=src_s3_key,  # output file name
                                output_video_mbps=4,              # Output video mbps.
                                downsample_ratio=None,            # A hyperparameter to adjust or use None for auto.
                                seq_chunk=12,                     # Frames chunk
                                # server_uri=server_uri_var,        # current server uri, required for full output link
                                generate_seg_video=True)          # required to save video for ML Dataset
        processing_log = f"{datetime.datetime.now()}: complete remover initialize  \n"
        for i, total in generator:
            if i % 50 == 0:
                processing_log = f"{datetime.datetime.now()}: {str(i)}/{total}\n"
                # SEND TO QUEUE

    except Exception as e:
        processing_log += f"Exception during process video \n\n{e}"
        # SEND TO QUEUE
        return
    
    processing_log = f"{datetime.datetime.now()}: start uploading to s3\n"
    upload_file_with_s3_key()
    processing_log += f"{datetime.datetime.now()}: complete uploaded to s3\n"
    
    s3_objects_processed = f"processed__{src_s3_key}"
    # SEND TO QUEUE


def upload_file_with_s3_key(src_s3_key, s3_bucket):
    s3_service = S3StorageService(s3_bucket)
    with open(f"/tmp/processed__{src_s3_key}", "rb") as file_obj:
        s3_service.upload(file_obj, s3_key=f"processed__{src_s3_key}", file_name=file_obj.name, public=True)


def download_file_with_s3_key(src_s3_key, s3_bucket) -> str:
    source_s3_storage = S3StorageService(s3_bucket)
    source_file = f"/tmp/{src_s3_key}"
    source_s3_storage.download_file(src_s3_key, source_file)
    
    return source_file