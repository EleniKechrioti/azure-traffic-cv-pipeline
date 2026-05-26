#!/bin/bash
# test_push.sh — στέλνει sample metrics στον Pushgateway για να τεστάρεις το Grafana
# Χρήση: bash test_push.sh

PUSHGATEWAY_URL="http://localhost:9091"

# --- CV Worker metrics (Q2, Q5, Q7, Q8) ---
curl -s --data-binary @- "${PUSHGATEWAY_URL}/metrics/job/cv_worker/instance/clip_01" <<EOF
# HELP avg_speed_per_lane Average speed km/h per lane per 5min window
# TYPE avg_speed_per_lane gauge
avg_speed_per_lane{lane="inbound",window="0-5min"} 72.3
avg_speed_per_lane{lane="inbound",window="5-10min"} 68.1
avg_speed_per_lane{lane="outbound",window="0-5min"} 81.5
avg_speed_per_lane{lane="outbound",window="5-10min"} 77.9
# HELP vehicle_count_per_lane Vehicle count per lane per 5min window
# TYPE vehicle_count_per_lane gauge
vehicle_count_per_lane{lane="inbound",window="0-5min"} 42
vehicle_count_per_lane{lane="inbound",window="5-10min"} 38
vehicle_count_per_lane{lane="outbound",window="0-5min"} 55
vehicle_count_per_lane{lane="outbound",window="5-10min"} 49
# HELP truck_ratio_per_lane Ratio of trucks per lane (0.0 - 1.0)
# TYPE truck_ratio_per_lane gauge
truck_ratio_per_lane{lane="inbound"} 0.18
truck_ratio_per_lane{lane="outbound"} 0.22
# HELP trucks_not_far_left_total Total trucks not driving in far-left lane
# TYPE trucks_not_far_left_total counter
trucks_not_far_left_total 14
# HELP clip_processing_duration_seconds Time to process one clip
# TYPE clip_processing_duration_seconds histogram
clip_processing_duration_seconds_bucket{le="10"} 0
clip_processing_duration_seconds_bucket{le="30"} 1
clip_processing_duration_seconds_bucket{le="60"} 1
clip_processing_duration_seconds_bucket{le="120"} 1
clip_processing_duration_seconds_bucket{le="+Inf"} 1
clip_processing_duration_seconds_sum 45.2
clip_processing_duration_seconds_count 1
EOF

# --- Alert Function metrics (Q3) ---
curl -s --data-binary @- "${PUSHGATEWAY_URL}/metrics/job/alert_function/instance/violations" <<EOF
# HELP speeders_total Total number of speeders by vehicle type
# TYPE speeders_total counter
speeders_total{type="car"} 7
speeders_total{type="truck"} 3
EOF

echo "✓ Metrics pushed. Άνοιξε http://localhost:3000 (admin/admin123)"
