from weather_api.api import get_pollution_data, get_location_data, get_weather_data
import json
import os
import sys
from requests.exceptions import SSLError
import time
from kafka.producer import CustomProducer
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo
import countries.europe as e
import scheduler as s


def schedule(scheduled_task):
    s.schedule(scheduled_task)

def get_api_data(latitude, longitude, api_key):
    pollution_data = get_pollution_data(latitude, longitude, api_key)
    weather_data = get_weather_data(latitude, longitude, api_key)
    try:
        country = weather_data['sys']['country']
    except KeyError:
        country = 'no country'

    message_json = {
        "pollution": pollution_data,
        "weather": weather_data
    }

    message = json.dumps(message_json)
    return {"message": message, "country": country}


def send_to_kafka(step):
    sys.stderr.write("Sending started ...")
    api_key = os.environ.get('API_KEY')
    producer = CustomProducer()

    count = 0
    start_time = datetime.now(ZoneInfo("Europe/Warsaw"))
    for latitude in np.arange(e.south_point, e.north_point, step):
        for longitude in np.arange(e.west_point, e.east_point, step):
            try:
                apid_data = get_api_data(latitude, longitude, api_key)
                message = apid_data['message']
                country = apid_data['country']

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
    end_time = datetime.now(ZoneInfo("Europe/Warsaw"))
    duration_in_seconds = (end_time - start_time).total_seconds()
    duration_in_minutes = int(duration_in_seconds / 60)

    start_time_string = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time_string = end_time.strftime('%Y-%m-%d %H:%M:%S')
    message = f'{count} messages were sent to Kafka at {start_time_string}, finished at {end_time_string}, lasted <b>{duration_in_minutes}</b> minutes'

    return message
