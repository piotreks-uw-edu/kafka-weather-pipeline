import requests

def get_pollution_data(latitude, longitude, app_id):
    base_url = "http://api.openweathermap.org/data/2.5/air_pollution"
    params = {
        "lat": latitude,
        "lon": longitude,
        "APPID": app_id
    }    
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()