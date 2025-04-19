from utils.load_env import env
from minio import Minio
from minio.error import S3Error
import os
import glob

def upload_local_directory_to_minio(minio_client: Minio, local_path, bucket_name, minio_path, overwrite):  
  for local_file in glob.glob(local_path + "/**"):
    file_name = os.path.basename(local_file)

    if os.path.isdir(local_file):      
      upload_local_directory_to_minio(
        minio_client,
        local_file,
        bucket_name,
        minio_path + file_name,
        overwrite
      )
    else:                
      remote_file = os.path.join(minio_path, file_name)
      found = minio_client.stat_object(bucket_name, remote_file)

      if not overwrite and found:
        return

      minio_client.fput_object(
        bucket_name=bucket_name,
        object_name=remote_file,
        file_path=local_file
      )

def upload_folder_to_minio(folder_path="data", overwrite=True):
  """Upload folder to minio bucket
  Args:
      folder_path (str, optional): Path to folder to be uploaded. Defaults to "data".
      overwrite (bool, optional): If true, overwrite existed object. Defaults to True
  """
  if not os.path.exists(folder_path):
    print(f"{folder_path} did not exist!")
    return

  try:    
    minio_client = Minio(
      endpoint=env.MINIO_ENDPOINT,
      access_key=env.MINIO_ROOT_USER,
      secret_key=env.MINIO_ROOT_PASSWORD,
      secure=False
    )

    folder_name = os.path.basename(folder_path)

    found = minio_client.bucket_exists(bucket_name=folder_name)
    if not found:
      minio_client.make_bucket(bucket_name=folder_name)
    else:
      print(f"Bucket {folder_name} is already existed")

    upload_local_directory_to_minio(
      minio_client=minio_client,
      local_path=folder_path,
      bucket_name=folder_name,      
      minio_path='',
      overwrite=overwrite
    )

    print(f"Successfully uploaded {folder_path} to MinIO bucket {folder_name}")
  except Exception as e:
    print(e)

if __name__ == "__main__":
  try:
    upload_folder_to_minio()
  except S3Error as exc:
    print("error occurred.", exc)