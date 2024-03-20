import os
import sys
import numpy as np
import json
import countries.europe as e
from weather_api.api import get_pollution_data, get_location_data
from kafka.producer import CustomProducer
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from collections import deque
from datetime import datetime
import random

app = Flask(__name__)

fifo_queue = deque(maxlen=10)

def scheduled_task():
    api_key = os.environ.get('API_KEY')
    producer = CustomProducer()
    step = random.randrange(800, 1000)/100
    count = 0
    start_time = datetime.now()
    for latitude in np.arange(e.south_point, e.north_point, step):
        for longitude in np.arange(e.west_point, e.east_point, step):
            pollution_data = get_pollution_data(latitude, longitude, api_key)
            location_data = get_location_data(latitude, longitude, api_key)
            country = location_data [0]['country'] if location_data else 'no country'

            message_json = {
                "pollution" : pollution_data,
                "location" : location_data
            }

            message = json.dumps(message_json)

            try:
                producer.produce(producer.topic,
                                 message,
                                 callback=producer.deliver_callback,
                                 key=country)
            except BufferError:
                sys.stderr.write(
                    f'Local Producer queue full ({len(p)} messages awaiting delivery) try again\n')
            
            # The call will return immediately without blocking
            producer.poll(0)

            count += 1

            if count % 100:
                producer.flush()
            
    producer.flush()
    end_time = datetime.now()
    duration_in_seconds = (end_time - start_time).total_seconds()
    duration_in_minutes = int(duration_in_seconds / 60)

    fifo_queue.append(f'{count} messages were sent to Kafka at {start_time}, finished at {end_time}, lasted <b>{duration_in_minutes}</b> minutes')


scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(scheduled_task, 'interval', minutes=30)
scheduler.start()

@app.route('/')
def home():
    return "<p>".join(list(fifo_queue))

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

