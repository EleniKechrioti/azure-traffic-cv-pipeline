import cv2
import numpy as np
from ultralytics import YOLO
import json

class TrafficAnalyzer:
    def __init__(self, model_path="yolov8n.pt"):
        self.model = YOLO(model_path)
        self.target_classes = [2, 5, 7] # 2=Car, 5=Bus, 7=Truck
        
        self.distance_y_meters = 17.0 # Height
        self.distance_x_meters = 25.0 # Width 
        self.alert_limit = 140.0
        
        # Bird's eye view transformation
        self.pts_src = np.float32([[100, 555], [332, 404], [914, 404], [1150, 555]])
        self.pts_dst = np.float32([[0, 170], [0, 0], [250, 0], [250, 170]])
        self.matrix = cv2.getPerspectiveTransform(self.pts_src, self.pts_dst)

    def get_lane_info(self, x_bird):
        """
        Calculates in which stream and lane (1-3) the vehicle is located.
        The width is 250 pixels.
        """
        lane_width = 110.0 / 3.0 # ~36.6 pixels per lane
        
        if x_bird < 125:
            stream = "Outbound"
            lane_num = int(min(x_bird, 109.9) / lane_width) + 1
        else:
            stream = "Inbound"
            adjusted_x = max(0, x_bird - 140) # Remove the nisida
            lane_num = int(min(adjusted_x, 109.9) / lane_width) + 1
            
        return stream, lane_num

    def process_clip(self, video_path, chunk_start_timestamp=0, alert_callback=None):
        """
        Processes a video clip and returns a list of detected vehicles with their speed and other info.
        """
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return []

        fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
        vehicle_history = {}
        results_list = []
        frame_count = 0

        while True:
            success, frame = cap.read()
            if not success:
                break
            
            frame_count += 1
            results = self.model.track(frame, persist=True, classes=self.target_classes, verbose=False)
            
            if results[0].boxes.id is not None:
                boxes = results[0].boxes.xyxy.cpu().numpy()
                track_ids = results[0].boxes.id.int().cpu().numpy()
                cls_ids = results[0].boxes.cls.int().cpu().numpy()

                for box, track_id, cls_id in zip(boxes, track_ids, cls_ids):
                    x_c = (box[0] + box[2]) / 2.0
                    y_c = box[3]
                    
                    p = np.array([[[x_c, y_c]]], dtype=np.float32)
                    transformed_p = cv2.perspectiveTransform(p, self.matrix)
                    x_bird = transformed_p[0][0][0]
                    y_bird = transformed_p[0][0][1]

                    # If we see a new track_id, we initialize its history
                    if track_id not in vehicle_history:
                        stream, lane_num = self.get_lane_info(x_bird)
                        vehicle_history[track_id] = {
                            'type': 'Truck' if cls_id in [5, 7] else 'Car',
                            'stream': stream,
                            'lane': lane_num,
                            'is_far_left': (lane_num == 1), 
                            'enter_frame': None,
                            'state': 'outside', # States: outside, inside, completed
                            'last_y': y_bird
                        }
                        continue # wait for the next frame to see movement

                    v_data = vehicle_history[track_id]
                    
                    if v_data['state'] == 'completed':
                        continue

                    last_y = v_data['last_y']
                    stream = v_data['stream']

                    if v_data['state'] == 'outside':
                        # For an Outbound to enter, it must have been below the line (>=170) before and now be above (<170)
                        if stream == "Outbound" and last_y >= 170 and y_bird < 170:
                            v_data['state'] = 'inside'
                            v_data['enter_frame'] = frame_count

                        # For an Inbound to enter, it must have been above the line (<=0) before and now be below (>0)
                        elif stream == "Inbound" and last_y <= 0 and y_bird > 0:
                            v_data['state'] = 'inside'
                            v_data['enter_frame'] = frame_count

                    elif v_data['state'] == 'inside':
                        # For an Outbound to exit, it must have been above the line (>0) before and now be below (<=0)
                        if stream == "Outbound" and last_y > 0 and y_bird <= 0:
                            frames_taken = frame_count - v_data['enter_frame']
                            if frames_taken > 0:
                                duration_sec = frames_taken / fps
                                speed_kmh = (self.distance_y_meters / duration_sec) * 3.6
                                
                                if 20 < speed_kmh < 250:
                                    real_timestamp = chunk_start_timestamp + (v_data['enter_frame'] / fps)
                                    event_data = {
                                        "vehicle_id": int(track_id),
                                        "type": v_data['type'],
                                        "stream": stream,
                                        "lane": v_data['lane'],
                                        "is_far_left": v_data['is_far_left'], 
                                        "speed_kmh": round(speed_kmh, 2),
                                        "timestamp": round(real_timestamp, 2)
                                    }
                                    results_list.append(event_data)
                                    
                                    if speed_kmh > self.alert_limit:
                                        if alert_callback is not None:
                                            alert_callback({
                                                "vehicle_id": int(track_id),
                                                "type": v_data['type'],
                                                "speed_kmh": round(speed_kmh, 2),
                                                "stream": stream,
                                                "timestamp": round(real_timestamp, 2)
                                            })
                            
                            v_data['state'] = 'completed'

                        # For an Inbound to exit, it must have been below the line (<170) before and now be above (>=170)
                        elif stream == "Inbound" and last_y < 170 and y_bird >= 170:
                            frames_taken = frame_count - v_data['enter_frame']
                            if frames_taken > 0:
                                duration_sec = frames_taken / fps
                                speed_kmh = (self.distance_y_meters / duration_sec) * 3.6
                                
                                if 20 < speed_kmh < 250:
                                    real_timestamp = chunk_start_timestamp + (v_data['enter_frame'] / fps)
                                    event_data = {
                                        "vehicle_id": int(track_id),
                                        "type": v_data['type'],
                                        "stream": stream,
                                        "lane": v_data['lane'],
                                        "is_far_left": v_data['is_far_left'], 
                                        "speed_kmh": round(speed_kmh, 2),
                                        "timestamp": round(real_timestamp, 2)
                                    }
                                    results_list.append(event_data)
                                    
                                    if speed_kmh > self.alert_limit:
                                        if alert_callback is not None:
                                            alert_callback(
                                                {
                                                    "vehicle_id": int(track_id),
                                                    "type": v_data['type'],
                                                    "speed_kmh": round(speed_kmh, 2),
                                                    "stream": stream,
                                                    "timestamp": round(real_timestamp, 2)
                                                }
                                            )
                            
                            v_data['state'] = 'completed'

                        
                        elif stream == "Outbound" and y_bird >= 170:
                            v_data['state'] = 'outside'
                        elif stream == "Inbound" and y_bird <= 0:
                            v_data['state'] = 'outside'

                    # update the last_y for the next iteration
                    v_data['last_y'] = y_bird

        cap.release()
        cv2.destroyAllWindows()
        return results_list
