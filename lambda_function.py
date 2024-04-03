import base64
import boto3
import os
import uuid
from concurrent.futures import ThreadPoolExecutor


def handle_error(e):
  print(e)
  return {
    "result": "FAIL",
    "data": None,
    "message": "사진이 업로드 되지 않았습니다.",
    "errorCode": "INTERNAL_SERVER_ERROR"
  }


def upload_image(image, resources):
  try:
    AWS_BUCKET_NAME = resources['AWS_BUCKET_NAME']
    bucket = resources['bucket']
    bucket_location = resources['bucket_location']

    location = 'images/'
    if image["location"] is not None:
      location = image["location"] + '/'
    path = location + str(uuid.uuid4()) + '.jpg'

    print("path set end")
    imageData = base64.b64decode(image["imageData"])
    print("imageData: ", imageData)

    bucket.put_object(
      ACL='public-read',
      Key=path,
      Body=imageData,
    )

    imageUrl = "https://s3-{0}.amazonaws.com/{1}/{2}".format(
      bucket_location['LocationConstraint'],
      AWS_BUCKET_NAME,
      path)

    return {
      "result": "SUCCESS",
      "data": {
        "imageUrl": imageUrl,
        "s3_path": path
      },
      "message": "이미지 업로드에 성공했습니다",
      "errorCode": None
    }
  except Exception as e:
    return handle_error(e)


def lambda_handler(event, context):
  try:
    print(event)
    images = event.get("images", [])
    if len(images) == 0:
      raise Exception("No images to upload")
    if len(images) > 10:
      raise Exception("Too many images")
  except Exception as e:
    return handle_error(e)

  AWS_BUCKET_NAME = os.environ.get('AWS_BUCKET_NAME')
  s3 = boto3.resource('s3')
  bucket = s3.Bucket(AWS_BUCKET_NAME)
  bucket_location = boto3.client('s3').get_bucket_location(Bucket=AWS_BUCKET_NAME)

  resources = {
    'AWS_BUCKET_NAME': AWS_BUCKET_NAME,
    'bucket': bucket,
    'bucket_location': bucket_location
  }

  with ThreadPoolExecutor(max_workers=50) as executor:
    results = list(executor.map(lambda image: upload_image(image, resources), images))

  if any(result["result"] == "FAIL" for result in results):
    successful_uploads = [result["data"]["s3_path"] for result in results if result["result"] == "SUCCESS"]

    if successful_uploads:
      print(f"deleting : {successful_uploads}")
      print(bucket.delete_objects(
        Delete={
          'Objects': [{'Key': path} for path in successful_uploads]
        }
      ))

    return handle_error(Exception("One or more images failed to upload"))

  return {
    "result": "SUCCESS",
    "data": [{"imageUrl": result["data"]["imageUrl"]} for result in results],
    "message": "이미지 업로드에 성공했습니다",
    "errorCode": None
  }
