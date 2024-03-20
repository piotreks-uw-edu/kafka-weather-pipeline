import sys
import os
import json  # Import the json module
from confluent_kafka import Consumer, KafkaError

# Load environment variables for Kafka configuration
bootstrap_servers = os.environ.get('BOOTSTAP_SERVERS')
security_protocol = os.environ.get('SECURITY_PROTOCOL')
sasl_mechanism = os.environ.get('SASL_MECHANISM')
sasl_username = os.environ.get('SASL_USERNAME')
client_id = os.environ.get('CLIENT_ID')
shared_access_key = os.environ.get('SHARED_ACCESS_KEY')
sasl_password = f"Endpoint=sb://{bootstrap_servers}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={shared_access_key}"
topic = os.environ.get('TOPIC')

conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': security_protocol,
    'sasl.mechanism': sasl_mechanism,
    'sasl.username': sasl_username,
    'sasl.password': sasl_password,
    'group.id': client_id,
}
if __name__ == "__main__":
    # Consumer configuration remains unchanged

    # Instantiate the Kafka Consumer with the configuration
    c = Consumer(**conf)
    
    # Subscribe to the topic
    c.subscribe([topic])

    try:
        while True:
            msg = c.poll(1.0)  # Wait for a message up to 1 second

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event remains unchanged
                    pass
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Deserialize the message value from string back to Python dictionary
                message_dict = json.loads(msg.value().decode("utf-8"))
                # Now you can work with message_dict as a normal Python dictionary
                sys.stderr.write(f'Received message: {message_dict}\n')
    except KeyboardInterrupt:
        pass
    finally:
        # Clean up on exit remains unchanged
        c.close()
