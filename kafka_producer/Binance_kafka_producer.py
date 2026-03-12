import json
import websocket
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
import sys
import time
# Cấu hình của Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'crypto_trade_price_1'
#danh sách các coin btc, eth, bnb, sol, xrp, ada, doge, shib
SYMBOLS = ['btcusdt', 'ethusdt', 'bnbusdt', 'solusdt', 'xrpusdt', 'adausdt', 'dogeusdt','shibusdt']
# URL Combined 
stream_string = "/".join([f"{symbol}@aggTrade" for symbol in SYMBOLS])

# Dùng endpoint '/stream?streams=' thay vì '/ws/'
BINANCE_SOCKET = f'wss://stream.binance.com:9443/stream?streams={stream_string}'

# Tạo topic nếu chưa tồn tại
def create_topic():
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    topic_list = [NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)]

    fs = admin_client.create_topics(topic_list)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
#Producer
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'binance-aggtrade-producer',
    'queue.buffering.max.messages': 1000000,
    'queue.buffering.max.ms': 1000,
    'compression.type': 'snappy'
}
producer = Producer(conf)

msg_count = 0

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        pass

# Websocket
def on_message(ws, message):
    try:
        raw_msg = json.loads(message)
        if 'data' not in raw_msg:
            return 
        data = raw_msg['data'] 

        processed_data = {
            "source": "binance_aggTrade",
            "symbol": data['s'],
            "price": float(data['p']),
            "quantity": float(data['q']),
            "trade_time": data['T'],
            "is_buyer_maker": data['m'],
            "ingested_at": datetime.now().isoformat()
        }

        producer.produce(
            KAFKA_TOPIC, 
            key=data['s'], 
            value=json.dumps(processed_data),
            callback=delivery_report
        )

    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection Closed")
    producer.flush()

def on_open(ws):
    print(f"Connected to Binance: {BINANCE_SOCKET}")

if __name__ == "__main__":
    create_topic()
    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_SOCKET,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except KeyboardInterrupt:
            print("Interrupted by user, closing...")
            break
        except Exception as e:
            print(f"Error creating WebSocketApp: {e}")
            time.sleep(5)

    print(f"Flushing producer...")
    producer.flush()
    print("Done.")