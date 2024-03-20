import os
import sys
import numpy as np
import json
import countries.europe as e
from weather_api.api import get_pollution_data, get_location_data
from kafka.producer import CustomProducer

api_key = os.environ.get('API_KEY')

if __name__ == "__main__":
    producer = CustomProducer()

    count = 0
    for latitude in np.arange(e.south_point, e.north_point, 10):
        for longitude in np.arange(e.west_point, e.east_point, 10):
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


