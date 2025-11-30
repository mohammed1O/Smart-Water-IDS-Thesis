import json
import pandas as pd
from collections import defaultdict, deque

# Read the JSON file
with open('moh.json', 'r') as file:
    data = json.load(file)

# Extract features
features = []
message_history = defaultdict(deque)  # Store message history per client
frame_history = defaultdict(deque)  # Store frame history for 5-minute frame count

for record in data:
    try:
        layers = record['_source']['layers']
        frame = layers['frame']
        eth = layers['eth']
        tcp = layers['tcp']
        mqtt = layers['mqtt']
        
        # Basic data extraction
        time_epoch = float(frame['frame.time_epoch'])
        frame_time_delta = float(frame['frame.time_delta'])
        tcp_len = int(tcp['tcp.len'])
        tcp_window_size = int(tcp['tcp.window_size'])
        mqtt_msgid = int(mqtt['mqtt.msgid'])
        mqtt_len = int(mqtt['mqtt.len'])
        mqtt_msgtype = mqtt.get('mqtt.hdrflags_tree', {}).get('mqtt.msgtype', '')
        
        # Extract source IP and MAC address
        ip_src = layers['ip']['ip.src']
        src_mac = eth['eth.src']
        
        # =============================================
        # Skip frames with packet_type in (5, 6, 7)
        # =============================================
        if mqtt_msgtype in ['5', '6', '7']:
            continue
        
        # Initialize sensor value
        sensor_value = None
        
        # Extract sensor value from MQTT payload for PUBLISH messages
        if mqtt_msgtype == '3' and 'mqtt.msg' in mqtt:
            try:
                payload_hex = mqtt['mqtt.msg']
                # Convert hex to bytes to string
                payload_bytes = bytes.fromhex(payload_hex.replace(':', ''))
                
                # Skip first 2 bytes (message ID for QoS 2)
                if len(payload_bytes) > 2:
                    payload_bytes = payload_bytes[2:]
                
                payload_str = payload_bytes.decode('utf-8', errors='ignore')
                
                # Try to parse as JSON and extract valor
                if payload_str:
                    # Add missing braces if needed
                    if not payload_str.startswith('{'):
                        payload_str = '{' + payload_str
                    if not payload_str.endswith('}'):
                        payload_str = payload_str + '}'
                    
                    payload_json = json.loads(payload_str)
                    sensor_value = payload_json.get('valor')
                    
            except Exception as e:
                print(f"Error extracting sensor value: {e}")
                sensor_value = None
        
        publish_rate = 0
        frame_count_5min = 0
        
        # If it's a PUBLISH message (type 3)
        if mqtt_msgtype == '3':
            # Update message history for this client
            message_history[ip_src].append(time_epoch)
            
            # Calculate publish rate (messages in last second)
            one_second_ago = time_epoch - 1.0
            while message_history[ip_src] and message_history[ip_src][0] < one_second_ago:
                message_history[ip_src].popleft()
            
            publish_rate = len(message_history[ip_src])
        
        # Calculate frame count in last 5 minutes for this IP
        frame_history[ip_src].append(time_epoch)
        
        five_minutes_ago = time_epoch - 300
        while frame_history[ip_src] and frame_history[ip_src][0] < five_minutes_ago:
            frame_history[ip_src].popleft()
        
        frame_count_5min = len(frame_history[ip_src])
        
        feature = {
            'time_epoch': time_epoch,
            'frame_time_delta': frame_time_delta,
            'tcp_len': tcp_len,
            'tcp_window_size': tcp_window_size,
            'mqtt_msgid': mqtt_msgid,
            'mqtt_len': mqtt_len,
            'mqtt_msgtype': mqtt_msgtype,
            'ip_src': ip_src,
            'src_mac': src_mac,
            'sensor_value': sensor_value,  # إضافة قيمة السنسر
            'publish_rate': publish_rate,
            'frame_count_5min': frame_count_5min
        }
        features.append(feature)
        
    except Exception as e:
        print(f"Error processing record: {e}")
        continue

# Save results to CSV
df = pd.DataFrame(features)
df.to_csv('11sssensor_data_features.csv', index=False)
print(f"Extracted {len(features)} records (excluding types 5, 6, 7)")

