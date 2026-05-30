import os
import tempfile
import logging
import json
import time
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from cv_process import TrafficAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CONN_STR    = os.environ["STORAGE_CONNECTION_STRING"]
EH_CONN_STR = os.environ["EVENTHUB_CONNECTION_STRING"]
EH_NAME     = os.environ.get("EVENTHUB_NAME", "clips-topic")
EH_GROUP    = os.environ.get("EVENTHUB_CONSUMER_GROUP", "cv-workers")

blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
analyzer = TrafficAnalyzer()


def get_clip_number(blob_name: str) -> int:
    """
    Εξάγει τον αύξοντα αριθμό από το όνομα του clip.
    π.χ. "clips-2min/clip_001.mp4" → 1
         "clip_003.mp4"            → 3
    """
    filename = blob_name.split("/")[-1]          # clip_001.mp4
    name = filename.replace(".mp4", "")          # clip_001
    number_str = name.split("_")[-1]             # 001
    return int(number_str)


def process_clip(blob_path: str):
    logging.info(f"Worker started for: {blob_path}")

    container_name, blob_name = blob_path.split("/", 1)

    # Υπολογισμός clip_start_sec από το όνομα του clip
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
            chunk_start_timestamp=clip_start_sec
        )
        latency = round(time.time() - start_time, 2)
        logging.info(f"CV processing done in {latency}s | vehicles detected: {len(results)}")

        # Κάνουμε τα vehicle IDs globally unique προσθέτοντας το clip prefix
        # π.χ. vehicle_id=3 στο clip_001 → "clip_001_3"
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


def on_event(partition_context, event):
    body = json.loads(event.body_as_str())
    blob_path = body["blob"]  # e.g. "clips-2min/clip_001.mp4"
    logging.info(f"Received event: {blob_path}")
    process_clip(blob_path)
    partition_context.update_checkpoint(event)


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