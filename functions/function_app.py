import logging
import os

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

TARGET_VIDEO = "Road traffic video for object recognition.mp4"
CLIPS_CONTAINER = "clips-2min"
RESULTS_CONTAINER = "cv-processor-results"


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


@app.blob_trigger(
    arg_name="myblob",
    path="cv-processor-results/{name}",
    connection="AzureWebJobsStorage",
)
def blob_trigger_analytics(myblob: func.InputStream):
    """Start the analytics job only once all clips have been processed.

    The CV workers run in parallel and each writes a '<clip>_results.json' to
    cv-processor-results. Analytics must aggregate across all clips (5-minute
    windows span multiple 2-minute clips), so we wait until the number of
    result files matches the number of 2-minute clips before starting the job.
    """
    blob_name = myblob.name.split("/")[-1]

    if not blob_name.endswith("_results.json"):
        logging.info("Ignoring blob '%s' — not a results file.", blob_name)
        return

    logging.info("CV result detected: %s (%d bytes)", blob_name, myblob.length)

    blob_service = BlobServiceClient.from_connection_string(
        os.environ["STORAGE_CONNECTION_STRING"]
    )

    clip_count = sum(
        1
        for b in blob_service.get_container_client(CLIPS_CONTAINER).list_blobs()
        if b.name.endswith(".mp4")
    )
    result_count = sum(
        1
        for b in blob_service.get_container_client(RESULTS_CONTAINER).list_blobs()
        if b.name.endswith("_results.json")
    )

    logging.info("Progress: %d/%d clips processed.", result_count, clip_count)

    if clip_count == 0 or result_count < clip_count:
        logging.info("Waiting for remaining clips before running analytics.")
        return

    logging.info("All %d clips processed — starting analytics job.", clip_count)

    credential = DefaultAzureCredential()
    client = ContainerAppsAPIClient(credential, os.environ["AZURE_SUBSCRIPTION_ID"])

    try:
        client.jobs.begin_start(
            resource_group_name=os.environ["AZURE_RESOURCE_GROUP"],
            job_name="vana-analytics-job",
        )
        logging.info("Analytics job started.")
    except Exception as e:
        logging.error(f"Failed to start analytics job: {e}")
        raise


@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="alerts-topic",
    connection="ALERTS_EVENTHUB_CONNECTION_STRING",
    consumer_group="alerts-subscribers",
)
def realtime_alert_logger(event: func.EventHubEvent):
    """Log each >140 km/h speed violation as it arrives (Q4)."""
    logging.warning("REAL-TIME ALERT: %s", event.get_body().decode("utf-8"))
