import os
import tempfile
import sys

from google.cloud import storage
import ffmpeg

storage_client = storage.Client()


def hvec2h264(data, context):
    file_data = data

    file_name = file_data['name']
    bucket_name = file_data['bucket']

    blob = storage_client.bucket(bucket_name).get_blob(file_name)

    print(f'Analyzing {file_name}.')

    return __convert_hvec2h264(blob)


def __convert_hvec2h264(current_blob):
    file_name = current_blob.name
    _, temp_local_filename_in = tempfile.mkstemp(suffix='.mp4')
    _, temp_local_filename_out = tempfile.mkstemp(suffix='.mp4')

    # Download file from bucket.
    current_blob.download_to_filename(temp_local_filename_in)
    print(f'Video {file_name} was downloaded to {temp_local_filename_in}.')

    try:
        (ffmpeg
            .input(temp_local_filename_in)
            .output(temp_local_filename_out, acodec='copy', vcodec='libx264', crf=18)
            .overwrite_output()
            .run()
         )
    except ffmpeg.Error as e:
        print(e.stderr, file=sys.stderr)
        sys.exit(1)

    print(f'Video {file_name} was transcoded.')

    # Upload result to a second bucket, to avoid re-triggering the function.
    output_bucket_name = os.getenv('OUTPUT_BUCKET_NAME')
    output_bucket = storage_client.bucket(output_bucket_name)
    new_blob = output_bucket.blob(file_name)
    new_blob.upload_from_filename(temp_local_filename_out)
    print(f'Converted video uploaded to: gs://{output_bucket_name}/{file_name}')

    # Delete the temporary files.
    os.remove(temp_local_filename_in)
    os.remove(temp_local_filename_out)

