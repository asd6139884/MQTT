import time
import configparser
# import mysql.connector
import paho.mqtt.client as mqtt

# 讀取設定檔
config = configparser.ConfigParser() 
config.read('control/Setting.ini')

## MQTT 設定
broker_address =  config.get('MQTT', 'broker_address')
broker_port =  config.getint('MQTT', 'broker_port')
username =  config.get('MQTT', 'username')
password =  config.get('MQTT', 'password')
use_tls =  config.getboolean('MQTT', 'use_tls')
forklifts_list = [vin.strip() for vin in config.get('MQTT', 'forklifts').split(';')]

## MySQL 設定
db_config = {
    "host": config.get('MySQL', 'host'),
    "user": config.get('MySQL', 'user'),
    "password": config.get('MySQL', 'password'),
    "database": config.get('MySQL', 'database'),
}
datatable = config.get('MySQL', 'datatable')

# 連線成功時訂閱所有堆高機的 Topic
def on_connect(client, userdata, flags, reason_code, properties):
    print("✅ Connected with reason code:", reason_code)
    if reason_code == 0:
        for vin in forklifts_list:
            topic = f"/ep/{vin}/data"
            client.subscribe(topic)
            print(f"📡 Subscribed to topic: {topic}")
    else:
        print("❌ Failed to connect. Reason code:", reason_code)


# 收到資料時處理
def on_message(client, userdata, msg):
    print(f"{client._client_id.decode()} received a message")
    print(f"📥 Received message on topic: {msg.topic}")
    print(f"📦 Payload: {msg.payload.decode()}")
    print(f"🚚 Forklift VIN: {msg.topic.split('/')[1]}")

    '''
    增加 收到資料時間 和 車輛序號
    '''


    # try:
    #     conn = mysql.connector.connect(**db_config)
    #     cursor = conn.cursor()

    #     vin = msg.topic.split("/")[1]
    #     topic = msg.topic
    #     payload = msg.payload.decode()

    #     sql = f"INSERT INTO {datatable} (vin, topic, payload) VALUES (%s, %s, %s)"
    #     cursor.execute(sql, (vin, topic, payload))
    #     conn.commit()
    #     print(f"✅ Saved message from {vin}")
    # except Exception as e:
    #     print(f"❌ MySQL Error: {e}")
    # finally:
    #     if conn.is_connected():
    #         cursor.close()
    #         conn.close()

# 斷線時自動重連
def on_disconnect(client, userdata, reason_code, properties):
    print("⚠️ Disconnected. Reason code:", reason_code)
    while True:
        try:
            client.reconnect()
            print("🔁 Reconnected.")
            break
        except Exception as e:
            print(f"❌ Reconnect failed: {e}. Retrying in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    while True:
        try:
            # 設定 MQTT Client
            client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            client.username_pw_set(username, password)
            if use_tls:
                client.tls_set()
            
            client.on_connect = on_connect
            client.on_message = on_message
            client.on_disconnect = on_disconnect

            client.connect(broker_address, broker_port, 60)
            client.loop_forever()
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            time.sleep(5)


