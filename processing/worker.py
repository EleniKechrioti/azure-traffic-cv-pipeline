import os
import tempfile
import logging
import json
from azure.storage.blob import BlobServiceClient
from cv_process import TrafficAnalyzer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CONN_STR = os.environ["STORAGE_CONNECTION_STRING"]
BLOB_PATH = os.environ["INPUT_BLOB_PATH"] 

blob_service_client = BlobServiceClient.from_connection_string(CONN_STR)
analyzer = TrafficAnalyzer()

def main():
    logging.info(f"Worker started for: {BLOB_PATH}")
    
    container_name, blob_name = BLOB_PATH.split("/", 1)
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_video_path = os.path.join(tmp_dir, blob_name)
        
        logging.info("Downloading video clip from Blob Storage...")
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(local_video_path, "wb") as f:
            f.write(blob_client.download_blob().readall())
            
        logging.info("Starting Computer Vision processing...")
        results = analyzer.process_clip(local_video_path, chunk_start_timestamp=0)
        
        result_blob_name = blob_name.replace(".mp4", "_results.json")
        results_json_str = json.dumps(results, indent=4)
        
        logging.info(f"Uploading results to container 'cv-processor-results' as {result_blob_name}...")
        res_blob_client = blob_service_client.get_blob_client(container="cv-processor-results", blob=result_blob_name)
        res_blob_client.upload_blob(results_json_str, overwrite=True)

if __name__ == "__main__":
    main()