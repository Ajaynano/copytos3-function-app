import azure.functions as func
import logging
import sys
import os
import boto3


from azure.storage.blob import BlobServiceClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body = req.get_json()
    azure_container_name = req_body.get("azureBlobContainer")
    aws_bucket_name = req_body.get("awsBucket")
    skip_if_exists = req_body.get("skipObjectsIfExists")

    name_starts_with_prefix =  None
    if req_body.get("nameStartsWith") is not None and req_body.get("nameStartsWith") != "":
        name_starts_with_prefix = req_body.get("nameStartsWith")

    connection_string = os.environ["BLOB_CONTAINER_CONNECTION_STRING"]
    aws_access_key_id = os.environ["AWS_KEY"]
    aws_secret_access_key = os.environ["AWS_SECRET"]

    # Connect to storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # get access to container
    container_client = blob_service_client.get_container_client(azure_container_name)

    # s3 client object
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # List all the blobs in the container, flattened - with the prefix
    blob_list = container_client.list_blobs(name_starts_with=name_starts_with_prefix)
    # for blob in blob_list:
    #         print(blob.name + '\n')
            
    # Copy blob to S3
    for blob in blob_list:
        if skip_if_exists:
            try: #ignore if object has already been copied. Save copy time.
                s3_client.head_object(Bucket=aws_bucket_name, Key=blob.name)
            except:
                s3_client.put_object(
                    Body=blob_service_client.get_blob_client(azure_container_name, blob.name).download_blob().readall(),
                    Bucket=aws_bucket_name,
                    Key=blob.name
                )
        else:
            s3_client.put_object(
                Body=blob_service_client.get_blob_client(azure_container_name, blob.name).download_blob().readall(),
                Bucket=aws_bucket_name,
                Key=blob.name
            )

    response_string = "Files copied successfully."
    return func.HttpResponse(response_string)