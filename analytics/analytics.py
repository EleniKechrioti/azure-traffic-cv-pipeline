import json
import argparse
import logging
import os
from collections import defaultdict
from prometheus_client import CollectorRegistry, Gauge, Histogram, push_to_gateway
from azure.storage.blob import BlobServiceClient
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

SPEED_LIMIT = {"Car": 90, "Truck": 80}

# Loggers
logger = logging.getLogger("analytics")
_speed_logger = logging.getLogger("avg_speed")
_speed_logger.propagate = False


def setup_loggers(output_dir: str, clip_id: str):
    os.makedirs(output_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(
                os.path.join(output_dir, f"{clip_id}_analytics.log"),
                encoding='utf-8'
            ),
            logging.StreamHandler(),
        ],
    )

    _speed_logger.setLevel(logging.INFO)
    speed_fh = logging.FileHandler(
        os.path.join(output_dir, f"{clip_id}_avg_speed.log"),
        encoding='utf-8'
    )
    speed_fh.setFormatter(logging.Formatter("%(message)s"))
    _speed_logger.addHandler(speed_fh)


# HELPERS

def load_data(path: str) -> tuple:
    """
    Φορτώνει το JSON που παράγει ο worker.
    Επιστρέφει (vehicles, clip_start_sec, processing_latency_sec).

    Υποστηρίζει δύο μορφές:
      - Νέα μορφή (worker με αλλαγές):
          {"clip_start_sec": 0, "processing_latency_sec": 14.3, "vehicles": [...]}
      - Παλιά μορφή (απευθείας list, για local testing):
          [{"vehicle_id": ..., ...}, ...]
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        # Παλιά μορφή — local testing με cv_results.json
        return data, 0.0, 0.0
    else:
        # Νέα μορφή — από worker
        vehicles = data.get("vehicles", [])
        clip_start_sec = data.get("clip_start_sec", 0.0)
        latency = data.get("processing_latency_sec", 0.0)
        return vehicles, clip_start_sec, latency


def download_from_blob(blob_path: str, local_path: str):
    """Κατεβάζει το JSON από Azure Blob Storage."""
    conn_str = os.environ["STORAGE_CONNECTION_STRING"]
    client = BlobServiceClient.from_connection_string(conn_str)
    container, blob_name = blob_path.split("/", 1)
    blob_client = client.get_blob_client(container=container, blob=blob_name)
    with open(local_path, "wb") as f:
        f.write(blob_client.download_blob().readall())
    logger.info(f"Downloaded {blob_path} → {local_path}")


def get_5min_window(timestamp_sec: float, clip_start_sec: float) -> int:
    return int((clip_start_sec + timestamp_sec) // 300)


def window_label(window_idx: int) -> str:
    start = window_idx * 5
    return f"{start}-{start + 5}min"


# QUERY 1 — Ταχύτητα κάθε οχήματος

def query1_vehicle_speeds(vehicles: list) -> dict:
    result = {v["vehicle_id"]: v["speed_kmh"] for v in vehicles}
    logger.info("=== QUERY 1: Ταχύτητα κάθε οχήματος ===")
    for vid, speed in sorted(result.items()):
        logger.info(f"  Vehicle {vid}: {speed:.2f} km/h")
    return result


# QUERY 2 — Ποσοστό φορτηγών ανά ρεύμα

def query2_truck_percentage(vehicles: list) -> dict:
    counts = defaultdict(lambda: {"total": 0, "trucks": 0})
    for v in vehicles:
        counts[v["stream"]]["total"] += 1
        if v["type"] == "Truck":
            counts[v["stream"]]["trucks"] += 1

    result = {}
    logger.info("=== QUERY 2: Ποσοστό φορτηγών ανά ρεύμα ===")
    for stream, data in counts.items():
        pct = (data["trucks"] / data["total"] * 100) if data["total"] > 0 else 0.0
        result[stream] = {**data, "pct": round(pct, 2)}
        logger.info(f"  {stream}: {data['trucks']}/{data['total']} φορτηγά ({pct:.2f}%)")
    return result


# QUERY 3 — Παραβάσεις ορίου ταχύτητας

def query3_speed_violations(vehicles: list) -> dict:
    violations = [v for v in vehicles if v["speed_kmh"] > SPEED_LIMIT[v["type"]]]
    logger.info("=== QUERY 3: Παραβάσεις ορίου ταχύτητας ===")
    logger.info(f"  Σύνολο παραβάσεων: {len(violations)}")
    for v in violations:
        limit = SPEED_LIMIT[v["type"]]
        logger.info(
            f"  Vehicle {v['vehicle_id']} ({v['type']}) — "
            f"{v['speed_kmh']:.2f} km/h > όριο {limit} km/h | "
            f"Stream: {v['stream']}, Lane: {v['lane']}"
        )
    return {"total": len(violations), "vehicles": violations}


# QUERY 5 — Αριθμός οχημάτων ανά ρεύμα και ανά 5λεπτο

def query5_vehicles_per_stream_per_window(vehicles: list, clip_start_sec: float) -> dict:
    counts = defaultdict(lambda: defaultdict(int))
    for v in vehicles:
        w = get_5min_window(v["timestamp"], clip_start_sec)
        counts[w][v["stream"]] += 1

    logger.info("=== QUERY 5: Αριθμός οχημάτων ανά ρεύμα και ανά 5λεπτο ===")
    result = {}
    for w in sorted(counts):
        label = window_label(w)
        result[label] = dict(counts[w])
        for stream, cnt in sorted(counts[w].items()):
            logger.info(f"  [{label}] {stream}: {cnt} οχήματα")
    return result


# QUERY 6 — Top 10 γρήγορα / αργά ανά ρεύμα και 5λεπτο

def query6_top_speeds(vehicles: list, clip_start_sec: float) -> dict:
    grouped = defaultdict(lambda: defaultdict(list))
    for v in vehicles:
        w = get_5min_window(v["timestamp"], clip_start_sec)
        grouped[w][v["stream"]].append({"id": v["vehicle_id"], "speed": v["speed_kmh"]})

    logger.info("=== QUERY 6: Top 10 πιο γρήγορα/αργά ανά ρεύμα και 5λεπτο ===")
    result = {}
    for w in sorted(grouped):
        label = window_label(w)
        result[label] = {}
        for stream, vehs in sorted(grouped[w].items()):
            top_fast = sorted(vehs, key=lambda x: x["speed"], reverse=True)[:10]
            top_slow = sorted(vehs, key=lambda x: x["speed"])[:10]
            result[label][stream] = {"top10_fast": top_fast, "top10_slow": top_slow}
            logger.info(f"  [{label}] {stream} — TOP 10 ΓΡΗΓΟΡΑ:")
            for e in top_fast:
                logger.info(f"    Vehicle {e['id']}: {e['speed']:.2f} km/h")
            logger.info(f"  [{label}] {stream} — TOP 10 ΑΡΓΑ:")
            for e in top_slow:
                logger.info(f"    Vehicle {e['id']}: {e['speed']:.2f} km/h")
    return result


# QUERY 7 — Μέση ταχύτητα ανά ρεύμα και ανά 5λεπτο

def query7_avg_speed(vehicles: list, clip_start_sec: float) -> dict:
    grouped = defaultdict(lambda: defaultdict(list))
    for v in vehicles:
        w = get_5min_window(v["timestamp"], clip_start_sec)
        grouped[w][v["stream"]].append(v["speed_kmh"])

    logger.info("=== QUERY 7: Μέση ταχύτητα ανά ρεύμα και ανά 5λεπτο ===")
    result = {}
    for w in sorted(grouped):
        label = window_label(w)
        result[label] = {}
        for stream, speeds in sorted(grouped[w].items()):
            avg = round(sum(speeds) / len(speeds), 2) if speeds else 0.0
            result[label][stream] = avg
            log_line = f"({stream.lower()}, {label}, {avg}km)"
            logger.info(f"  {log_line}")      # → analytics.log + console
            _speed_logger.info(log_line)       # → avg_speed.log μόνο
    return result


# QUERY 8 — Φορτηγά εκτός αριστερής λωρίδας

def query8_trucks_not_far_left(vehicles: list) -> dict:
    wrong = [v for v in vehicles if v["type"] == "Truck" and not v["is_far_left"]]
    logger.info("=== QUERY 8: Φορτηγά εκτός τέρμα αριστερής λωρίδας ===")
    logger.info(f"  Σύνολο: {len(wrong)}")
    for v in wrong:
        logger.info(f"  Vehicle {v['vehicle_id']} — Stream: {v['stream']}, Lane: {v['lane']}")
    return {"total": len(wrong), "vehicles": wrong}


# PROMETHEUS PUSH

def push_to_prometheus(clip_id, q2, q3, q5, q7, q8, processing_latency_sec, pushgateway_url):
    registry = CollectorRegistry()

    g_truck_pct = Gauge("truck_percentage_per_stream", "Ποσοστό φορτηγών ανά ρεύμα",
                        ["stream"], registry=registry)
    for stream, data in q2.items():
        g_truck_pct.labels(stream=stream).set(data["pct"])

    Gauge("speed_violations_total", "Παραβάσεις ορίου ταχύτητας",
          registry=registry).set(q3["total"])

    g_veh = Gauge("vehicles_per_stream_per_window", "Οχήματα ανά ρεύμα και 5λεπτο",
                  ["stream", "window"], registry=registry)
    for w_label, streams in q5.items():
        for stream, cnt in streams.items():
            g_veh.labels(stream=stream, window=w_label).set(cnt)

    g_avg = Gauge("avg_speed_kmh_per_stream_per_window", "Μέση ταχύτητα ανά ρεύμα και 5λεπτο",
                  ["stream", "window"], registry=registry)
    for w_label, streams in q7.items():
        for stream, avg in streams.items():
            g_avg.labels(stream=stream, window=w_label).set(avg)

    Gauge("trucks_not_far_left_total", "Φορτηγά εκτός αριστερής λωρίδας",
          registry=registry).set(q8["total"])

    h = Histogram("cv_chunk_processing_seconds", "Latency επεξεργασίας clip (CV Worker)",
                  buckets=[5, 10, 15, 20, 30, 45, 60, 90, 120], registry=registry)
    h.observe(processing_latency_sec)

    try:
        push_to_gateway(pushgateway_url, job=f"analytics_{clip_id}", registry=registry)
        logger.info(f"Metrics pushed to Prometheus για {clip_id}")
    except Exception as e:
        logger.error(f"Αποτυχία push στο Prometheus: {e}")


# MAIN

def main():
    parser = argparse.ArgumentParser(description="Traffic Analytics — queries 1-3, 5-8")
    parser.add_argument("--input", default=None,
                        help="Local path στο JSON (για local testing). "
                             "Αν δεν οριστεί, χρησιμοποιείται το BLOB_INPUT_PATH env var.")
    parser.add_argument("--clip_id", default=os.environ.get("CLIP_ID", "clip_01"),
                        help="Αναγνωριστικό clip (default: clip_01 ή env CLIP_ID)")
    parser.add_argument("--push_gw", default=os.environ.get("PUSHGATEWAY_URL", "http://localhost:9091"),
                        help="URL Prometheus Pushgateway")
    parser.add_argument("--output_dir", default="output",
                        help="Φάκελος για τα log files (default: output/)")
    args = parser.parse_args()

    setup_loggers(args.output_dir, args.clip_id)

    # Αν δεν δοθεί --input, κατεβάζουμε από Blob (Azure mode)
    if args.input:
        local_path = args.input
        logger.info(f"Local mode: φόρτωση από {local_path}")
    else:
        blob_path = os.environ["BLOB_INPUT_PATH"]  # π.χ. "cv-processor-results/clip_01_results.json"
        local_path = f"/tmp/{args.clip_id}_results.json"
        logger.info(f"Azure mode: κατέβασμα από Blob {blob_path}")
        download_from_blob(blob_path, local_path)

    vehicles, clip_start_sec, latency = load_data(local_path)
    logger.info(f"Σύνολο εγγραφών: {len(vehicles)} | clip_start: {clip_start_sec}s | latency: {latency}s")

    q1 = query1_vehicle_speeds(vehicles)
    q2 = query2_truck_percentage(vehicles)
    q3 = query3_speed_violations(vehicles)
    q5 = query5_vehicles_per_stream_per_window(vehicles, clip_start_sec)
    q6 = query6_top_speeds(vehicles, clip_start_sec)
    q7 = query7_avg_speed(vehicles, clip_start_sec)
    q8 = query8_trucks_not_far_left(vehicles)

    push_to_prometheus(
        clip_id=args.clip_id,
        q2=q2, q3=q3, q5=q5, q7=q7, q8=q8,
        processing_latency_sec=latency,
        pushgateway_url=args.push_gw,
    )

    logger.info("Analytics ολοκληρώθηκαν.")


if __name__ == "__main__":
    main()