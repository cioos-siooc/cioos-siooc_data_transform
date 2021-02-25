from shapely.geometry import Point, Polygon
import json


def read_geojson(filename):
    # read shapefile in geojson format into Polygon object
    # input geojson file
    # output: Polygon object
    with open(filename) as f:
        data = json.load(f)
    poly_dict = {}

    for feature in data["features"]:
        if feature["geometry"]["type"] == "Polygon":
            # print(feature['geometry']['coordinates'][0])
            p = Polygon(feature["geometry"]["coordinates"][0])
            name = feature["properties"]["name"]
            poly_dict[name] = p
    return poly_dict


def is_in_polygon(polygon, point):
    # identify if point is inside polygon
    return polygon.contains(point)


def find_geographic_area(poly_dict, point):
    name_str = ""
    for key in poly_dict:
        if is_in_polygon(poly_dict[key], point):
            name_str = "{}{} ".format(name_str, key.replace(" ", "-"))
            # print(name_str)
    return name_str


def get_geo_code(location, polygons_dict):
    # read geojson file and assign file
    geo_code = find_geographic_area(
        polygons_dict,
        Point(location[0], location[1]),
    )
    if geo_code == "":
        geo_code = "n/a"

    # print(f"geocode = {geo_code}; lonlat = {location}")
    return geo_code
