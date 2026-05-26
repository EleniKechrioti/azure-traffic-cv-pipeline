import os
import subprocess
import tempfile
import logging
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubProducerClient, EventData

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CONN_STR    = os.environ["STORAGE_CONNECTION_STRING"]
EH_CONN_STR = os.environ["EVENTHUB_CONNECTION_STRING"]
EH_NAME     = os.environ["EVENTHUB_NAME"]          # e.g. "clips-topic"
BLOB_NAME   = os.environ["BLOB_NAME"]               # e.g. "Road traffic video ..."
SEGMENT_SEC = 120                                   # 2-minute clips

blob_client = BlobServiceClient.from_connection_string(CONN_STR)

def download_video(tmp_dir: str) -> str:
    local_path = os.path.join(tmp_dir, BLOB_NAME)
    logging.info("Downloading %s ...", BLOB_NAME)
    data = blob_client.get_blob_client("input-videos", BLOB_NAME).download_blob().readall()
    with open(local_path, "wb") as f:
        f.write(data)
    logging.info("Downloaded %d MB", len(data) // 1_000_000)
    return local_path

def split_video(video_path: str, tmp_dir: str) -> list[str]:
    out_pattern = os.path.join(tmp_dir, "clip_%03d.mp4")
    subprocess.run([
        "ffmpeg", "-i", video_path,
        "-c", "copy",
        "-f", "segment", "-segment_time", str(SEGMENT_SEC),
        "-reset_timestamps", "1",
        out_pattern,
    ], check=True)
    clips = sorted(Path(tmp_dir).glob("clip_*.mp4"))
    logging.info("Split into %d clips", len(clips))
    return [str(c) for c in clips]

def upload_and_notify(clips: list[str]) -> None:
    producer = EventHubProducerClient.from_connection_string(EH_CONN_STR, eventhub_name=EH_NAME)
    with producer:
        for clip_path in clips:
            clip_name = os.path.basename(clip_path)
            # Upload clip
            with open(clip_path, "rb") as f:
                blob_client.get_blob_client("clips-2min", clip_name).upload_blob(f, overwrite=True)
            logging.info("Uploaded %s", clip_name)
            # Publish event
            batch = producer.create_batch()
            batch.add(EventData(f'{{"blob":"clips-2min/{clip_name}"}}'))
            producer.send_batch(batch)
            logging.info("Event published for %s", clip_name)

with tempfile.TemporaryDirectory() as tmp:
    video = download_video(tmp)
    clips = split_video(video, tmp)
    upload_and_notify(clips)

logging.info("Preprocessing complete.")
