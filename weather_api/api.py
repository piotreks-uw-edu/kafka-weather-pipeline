import requests


def get_response(base_url, params):
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()    


def get_pollution_data(latitude, longitude, app_id):
    base_url = "http://api.openweathermap.org/data/2.5/air_pollution"
    params = {
        "lat": latitude,
        "lon": longitude,
        "APPID": app_id
    }
    return get_response(base_url, params)


def get_location_data(latitude, longitude, app_id):
    base_url = "http://api.openweathermap.org/geo/1.0/reverse"
    params = {
        "lat": latitude,
        "lon": longitude,
        "APPID": app_id
    }
    return get_response(base_url, params)


def get_weather_data(latitude, longitude, app_id):
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": latitude,
        "lon": longitude,
        "APPID": app_id
    }
    return get_response(base_url, params)