import os
import sys
import numpy as np
import json
import countries.europe as e
from weather_api.api import get_pollution_data, get_location_data, get_weather_data
from kafka.producer import CustomProducer
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from collections import deque
from datetime import datetime
import random
from requests.exceptions import SSLError
import time

app = Flask(__name__)

fifo_queue = deque(maxlen=10)


def scheduled_task():
    api_key = os.environ.get('API_KEY')
    producer = CustomProducer()
    step = random.randrange(800, 1000)/1000
    count = 0
    start_time = datetime.now()
    for latitude in np.arange(e.south_point, e.north_point, step):
        for longitude in np.arange(e.west_point, e.east_point, step):
            try:
                pollution_data = get_pollution_data(latitude, longitude, api_key)
                # location_data = get_location_data(latitude, longitude, api_key)
                weather_data = get_weather_data(latitude, longitude, api_key)
                try:
                    country = weather_data['sys']['country']
                except KeyError:
                    country = 'no country'

                message_json = {
                    "pollution": pollution_data,
                    # "location": location_data,
                    "weather": weather_data
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
            except SSLError as ex:
                print("An SSL error occurred:", ex)
                time.sleep(30)
            except Exception as ex:
                print("An unexpected error occurred:", ex)
                time.sleep(30)

    producer.flush()
    end_time = datetime.now()
    duration_in_seconds = (end_time - start_time).total_seconds()
    duration_in_minutes = int(duration_in_seconds / 60)

    fifo_queue.append(f'{count} messages were sent to Kafka at {start_time}, finished at {end_time}, lasted <b>{duration_in_minutes}</b> minutes')


scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(scheduled_task, 'interval', hours=1)

scheduler.start()

fifo_queue.append(f'Started at {datetime.now()}')


@app.route('/')
def home():
    return "<p>".join(list(fifo_queue))

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

