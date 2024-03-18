import os
import numpy as np
import countries.poland as p
from weather_api.pollution import get_pollution_data

api_key = os.environ.get('API_KEY')

if __name__ == "__main__":
    for latitude in np.arange(p.south_point, p.north_point, 1):
        for longitude in np.arange(p.west_point, p.east_point, 1):
            pollution_data = get_pollution_data(latitude, longitude, api_key)
            print(pollution_data)