import sys, os
from datetime import datetime
from confluent_kafka import Producer

# Load environment variables for Kafka configuration
bootstrap_servers = os.environ.get('BOOTSTAP_SERVERS')
security_protocol = os.environ.get('SECURITY_PROTOCOL')
sasl_mechanism = os.environ.get('SASL_MECHANISM')
sasl_username = os.environ.get('SASL_USERNAME')
client_id = os.environ.get('CLIENT_ID')
shared_access_key = os.environ.get('SHARED_ACCESS_KEY')
sasl_password=f"Endpoint=sb://{bootstrap_servers}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={shared_access_key}"
topic = os.environ.get('TOPIC')

if __name__ == "__main__":
    conf = {
        'bootstrap.servers' : bootstrap_servers,
        'security.protocol' : security_protocol,
        'sasl.mechanism' : sasl_mechanism,
        'sasl.username' : sasl_username,
        'sasl.password' : sasl_password,
        'client.id' : client_id,
    }

    # Instantiate the Kafka Producer with the configuration
    p = Producer(**conf)

    def deliver_callback(err, msg):
        if err:
            sys.stderr.write(f'Message failed delivery: {err}\n')
        else:
            sys.stderr.write(f'Message delivered to topic= {msg.topic()}, partition= [{msg.partition()}], offset= {msg.offset():o}\n')

    # Produce messages to the Kafka topic
    for i in range(0,100):
        key = "even" if i%2 == 0 else "odd"
        try:
            message = str(i) + ' ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            p.produce(topic, message, callback=deliver_callback, key=key)
        except BufferError as e:
            sys.stderr.write(f'Local Producer queue full ({len(p)} messages awaiting delivery) try again\n')

        # the call will return immediately without blocking
        p.poll(0)

    sys.stderr.write(f'Waiting for {len(p)} deliveries\n')

    p.flush()