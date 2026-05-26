import logging
import os

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient

app = func.FunctionApp()

TARGET_VIDEO = "Road traffic video for object recognition.mp4"


@app.blob_trigger(
    arg_name="myblob",
    path="input-videos/{name}",
    connection="AzureWebJobsStorage",
)
def blob_trigger_preprocessing(myblob: func.InputStream):
    blob_name = myblob.name.split("/")[-1]

    if blob_name != TARGET_VIDEO:
        logging.info("Ignoring blob '%s' — not the target video.", blob_name)
        return

    logging.info("Target video detected: %s (%d bytes)", blob_name, myblob.length)

    blob_uri = (
        f"https://{os.environ['STORAGE_ACCOUNT_NAME']}.blob.core.windows.net"
        f"/input-videos/{blob_name}"
    )

    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, os.environ["AZURE_SUBSCRIPTION_ID"])

    client.jobs.begin_start(
        resource_group_name=os.environ["AZURE_RESOURCE_GROUP"],
        job_name=os.environ["PREPROCESSING_JOB_NAME"],
        template={
            "containers": [{
                "name": "preprocessor",
                "image": "kanellaman/vana-preprocessor:latest",
                "env": [
                    {"name": "INPUT_BLOB_URI", "value": blob_uri},
                    {"name": "BLOB_NAME", "value": blob_name},
                    {"name": "STORAGE_CONNECTION_STRING", "value": os.environ["STORAGE_CONNECTION_STRING"]},
                    {"name": "EVENTHUB_CONNECTION_STRING", "value": os.environ["EVENTHUB_CONNECTION_STRING"]},
                    {"name": "EVENTHUB_NAME", "value": os.environ["EVENTHUB_NAME"]},
                ],
            }]
        },
    )

    logging.info("Preprocessing job started for '%s'.", blob_name)
