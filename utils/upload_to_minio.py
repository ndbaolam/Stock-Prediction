from load_env import env
from minio import Minio
from minio.error import S3Error
import os
import glob
import argparse

def upload_local_directory_to_minio(minio_client: Minio, local_path, bucket_name, minio_path, overwrite):  
  for local_file in glob.glob(local_path + "/**", recursive=True):
    if os.path.isdir(local_file):
      continue

    relative_path = os.path.relpath(local_file, local_path)
    remote_file = os.path.join(minio_path, relative_path).replace("\\", "/")

    file_exists = False
    try:
      minio_client.stat_object(bucket_name, remote_file)
      file_exists = True
    except S3Error as e:
      if e.code == "NoSuchKey":
        file_exists = False
      else:
        raise e

    if not overwrite and file_exists:
      print(f"Skipping existing file: {remote_file}")
      continue

    print(f"Uploading {local_file} -> {bucket_name}/{remote_file}")
    minio_client.fput_object(
      bucket_name=bucket_name,
      object_name=remote_file,
      file_path=local_file
    )

def upload_folder_to_minio(folder_path="data", overwrite=True):
  """Upload folder to MinIO bucket"""
  if not os.path.exists(folder_path):
    print(f"❌ Error: {folder_path} does not exist!")
    return

  try:
    minio_client = Minio(
      endpoint=env.MINIO_ENDPOINT,
      access_key=env.MINIO_ROOT_USER,
      secret_key=env.MINIO_ROOT_PASSWORD,
      secure=False
    )

    folder_name = os.path.basename(folder_path.rstrip('/')).lower()
    
    if len(folder_name) < 3:
      folder_name = f"bucket-{folder_name}"

    folder_name = folder_name.replace('_', '-') 

    found = minio_client.bucket_exists(bucket_name=folder_name)
    if not found:
      minio_client.make_bucket(bucket_name=folder_name)
      print(f"🪣 Bucket '{folder_name}' created.")
    else:
      print(f"✅ Bucket '{folder_name}' already exists.")

    upload_local_directory_to_minio(
      minio_client=minio_client,
      local_path=folder_path,
      bucket_name=folder_name,
      minio_path='',
      overwrite=overwrite
    )

    print(f"✅ Successfully uploaded {folder_path} to MinIO bucket '{folder_name}'")
  except Exception as e:
    print("❌ Error occurred:", e)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Upload a folder to MinIO bucket.")
  parser.add_argument("folder_path", type=str, help="Path to the folder to upload")
  parser.add_argument("--overwrite", type=bool, default=True, help="Overwrite existing files (default: True)")
  args = parser.parse_args()

  try:
    upload_folder_to_minio(folder_path=args.folder_path, overwrite=args.overwrite)
  except S3Error as exc:
    print("S3 error occurred:", exc)
  except Exception as exc:
    print("General error occurred:", exc)