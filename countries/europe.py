north_point = 67.09803998799359
south_point = 36.11895420075611
west_point = -5.557125374268824
east_point = 41.40564145542043
step = 0.1


def get_number_of_api_calls(north_point, south_point, west_point, east_point,
                            step):
    in_latitude = (north_point - south_point) / step
    in_longitude = (east_point - west_point) / step
    return in_latitude * in_longitude


def get_region(latitude, longitude, north_point, south_point, west_point,
               east_point):
    if latitude < ((north_point - south_point)/2 + south_point):
        lat = 'south'
    else:
        lat = 'north'
    if longitude < ((east_point - west_point)/2 + west_point):
        lon = 'west'
    else:
        lon = 'east'
    return f'{lat}_{lon}'
    

# print(get_number_of_api_calls(north_point, south_point, west_point,
# east_point, step))
# print(get_region(63.81267324116743, 28.598205971821308, north_point,
# south_point, west_point, east_point))