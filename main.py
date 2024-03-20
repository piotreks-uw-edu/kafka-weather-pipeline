import os
import numpy as np
import json
import countries.europe as e
from weather_api.pollution import get_pollution_data
from kafka.producer import CustomProducer

api_key = os.environ.get('API_KEY')

if __name__ == "__main__":
    producer = CustomProducer()

    count = 0
    for latitude in np.arange(e.south_point, e.north_point, 1):
        for longitude in np.arange(e.west_point, e.east_point, 1):
            pollution_json = get_pollution_data(latitude, longitude, api_key)
            message = json.dumps(pollution_json)

            key = e.get_region(latitude,
                               longitude,
                               e.north_point,
                               e.south_point,
                               e.west_point,
                               e.east_point)

            try:
                producer.produce(producer.topic,
                                message,
                                callback=producer.deliver_callback,
                                key=key)
            except BufferError:
                sys.stderr.write(
                    f'Local Producer queue full ({len(p)} messages awaiting delivery) try again\n')
            
            # The call will return immediately without blocking
            producer.poll(0)

            count += 1

            if count % 100:
                producer.flush()
            
    producer.flush()


