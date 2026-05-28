import os
import tempfile
import logging
import json
import time
from azure.storage.blob import BlobServiceClient
from cv_process import TrafficAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CONN_STR = os.environ["STORAGE_CONNECTION_STRING"]
BLOB_PATH = os.environ["INPUT_BLOB_PATH"]
CLIP_START_SEC = float(os.environ.get("CLIP_START_SEC", 0))

blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
analyzer = TrafficAnalyzer()

def main():
    logging.info(f"Worker started for: {BLOB_PATH}")

    container_name, blob_name = BLOB_PATH.split("/", 1)

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_video_path = os.path.join(tmp_dir, blob_name)

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
            chunk_start_timestamp=CLIP_START_SEC
        )
        latency = round(time.time() - start_time, 2)
        logging.info(f"CV processing done in {latency}s")

        # Προσθέτουμε το latency στο JSON ώστε το analytics να το πάρει
        output = {
            "clip_start_sec": CLIP_START_SEC,
            "processing_latency_sec": latency,
            "vehicles": results
        }

        result_blob_name = blob_name.replace(".mp4", "_results.json")
        logging.info(f"Uploading results to cv-processor-results/{result_blob_name}...")
        res_blob_client = blob_service_client.get_blob_client(
            container="cv-processor-results", blob=result_blob_name
        )
        res_blob_client.upload_blob(json.dumps(output, indent=4), overwrite=True)
        logging.info("Done.")

if __name__ == "__main__":
    main()