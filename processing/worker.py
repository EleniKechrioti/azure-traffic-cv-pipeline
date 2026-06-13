import os
import tempfile
import logging
import json
import time
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubConsumerClient, EventHubProducerClient, EventData
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from cv_process import TrafficAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CONN_STR    = os.environ["STORAGE_CONNECTION_STRING"]
EH_CONN_STR = os.environ["EVENTHUB_CONNECTION_STRING"]
EH_NAME     = os.environ.get("EVENTHUB_NAME", "clips-topic")
EH_GROUP    = os.environ.get("EVENTHUB_CONSUMER_GROUP", "cv-workers")

ALERTS_CONN_STR = os.environ.get("ALERTS_EVENTHUB_CONNECTION_STRING")
ALERTS_EH_NAME  = os.environ.get("ALERTS_EVENTHUB_NAME", "alerts-topic")

blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
analyzer = TrafficAnalyzer()

alert_producer = None
if ALERTS_CONN_STR:
    alert_producer = EventHubProducerClient.from_connection_string(
        conn_str=ALERTS_CONN_STR, eventhub_name=ALERTS_EH_NAME
    )

def get_clip_number(blob_name: str) -> int:
    """
    Extracts the clip number from the blob name.
    """
    filename = blob_name.split("/")[-1]          # clip_001.mp4
    name = filename.replace(".mp4", "")          # clip_001
    number_str = name.split("_")[-1]             # 001
    return int(number_str)


def process_clip(blob_path: str):
    logging.info(f"Worker started for: {blob_path}")

    container_name, blob_name = blob_path.split("/", 1)

    # Calculate start timestamp from clip number
    clip_number = get_clip_number(blob_name)
    clip_start_sec = clip_number * 120.0
    clip_id = f"clip_{clip_number:03d}"
    logging.info(f"Clip number: {clip_number} | clip_start_sec: {clip_start_sec}s")

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_video_path = os.path.join(tmp_dir, blob_name.split("/")[-1])

        logging.info("Downloading video clip from Blob Storage...")
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        with open(local_video_path, "wb") as f:
            f.write(blob_client.download_blob().readall())

        logging.info("Starting Computer Vision processing...")
        start_time = time.time()
        results = analyzer.process_clip(
            local_video_path,
            chunk_start_timestamp=clip_start_sec,
            alert_callback=handle_realtime_alert
        )
        latency = round(time.time() - start_time, 2)
        logging.info(f"CV processing done in {latency}s | vehicles detected: {len(results)}")

        # Make vehicle IDs globally unique by adding the clip prefix
        # e.g., vehicle_id=3 in clip_001 → "clip_001_3"
        for vehicle in results:
            vehicle["vehicle_id"] = f"{clip_id}_{vehicle['vehicle_id']}"

        output = {
            "clip_id": clip_id,
            "clip_start_sec": clip_start_sec,
            "processing_latency_sec": latency,
            "vehicles": results
        }

        result_blob_name = blob_name.split("/")[-1].replace(".mp4", "_results.json")
        logging.info(f"Uploading results to cv-processor-results/{result_blob_name}...")
        res_blob_client = blob_service_client.get_blob_client(
            container="cv-processor-results", blob=result_blob_name
        )
        res_blob_client.upload_blob(json.dumps(output, indent=4), overwrite=True)
        logging.info("Done.")

def handle_realtime_alert(alert_data):
    if alert_producer:
        try:
            batch = alert_producer.create_batch()
            batch.add(EventData(json.dumps(alert_data)))
            alert_producer.send_batch(batch)
            logging.info(f"Real-time alert sent to Azure: {alert_data}")
        except Exception as e:
            logging.error(f"Failed to send alert: {e}")

def on_event(partition_context, event):
    body = json.loads(event.body_as_str())
    blob_path = body["blob"]  # e.g. "clips-2min/clip_001.mp4"
    logging.info(f"Received event: {blob_path}")
    
    # Commit offset IMMEDIATELY — don't wait for processing
    # This allows other replicas to read next message while we process
    partition_context.update_checkpoint(event)
    logging.info(f"Checkpoint committed for {blob_path} — other replicas can now read next message")
    
    # Now process the clip (other replicas process in parallel)
    try:
        process_clip(blob_path)
    except Exception as e:
        logging.error(f"Failed to process {blob_path}: {e}")
        # Note: offset already committed, so this message won't be retried

def main():
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        CONN_STR, container_name="eventhub-checkpoints"
    )
    client = EventHubConsumerClient.from_connection_string(
        EH_CONN_STR, consumer_group=EH_GROUP, eventhub_name=EH_NAME,
        checkpoint_store=checkpoint_store
    )
    logging.info(f"Listening on {EH_NAME} / {EH_GROUP} ...")
    with client:
        client.receive(on_event=on_event, starting_position="-1")


if __name__ == "__main__":
    main()