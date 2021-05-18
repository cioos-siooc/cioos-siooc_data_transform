from shapely.geometry import Polygon, Point
import json
import os


# general utility functions common to multiple classes
def fix_path(path):
    # converts path from posix to nt if system is nt
    # input is string with path in posix format '/' file sep
    if os.name == "nt":
        path = os.path.sep.join(path.split("/"))
    return path


def is_in(keywords, string):
    # simple function to check if any keyword is in string
    # convert string and keywords to upper case before checking
    return any([string.upper().find(z.upper()) >= 0 for z in keywords])


def read_config(config_file):
    # read json file with information on dataset etc.
    with open(config_file) as fid:
        config = json.load(fid)
        return config


def import_env_variables(filename="./.env"):
    # import information in file to a dictionary
    # this file makes the implementation independent of local folder structure
    # data in file should be key:value pairs. Key should be unique
    info = {}
    with open(filename, "r") as fid:
        lines = fid.readlines()
        for line in lines:
            if line.strip() == "":
                break
            elif line.strip()[0] == "#":
                continue
            info[line.split(":")[0].strip()] = line.split(":")[1].strip()
    return info


def file_mod_time(filename):
    # returns how old the file is based on timestamp
    # returns the time in hours
    import time
    import os

    dthrs = (os.path.getmtime(filename) - time.time()) / 3600.0
    return dthrs


def release_memory(outfile):
    # release memory from file and variable class created.
    for c in outfile.varlist:
        del c
    del outfile


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


def compare_file_list(sub_set, global_set, opt="not-in"):
    from itertools import compress

    # compares files in sub_set and global_set to find strings from global_set that are 'not-in' or 'in' sub_set
    # inputs are two lists: sub_set and global_set
    # options: 'not-in' [default] and 'in'
    # extensions are removed if present in the lists provided as inputs
    # mar 02 2020 edit: Pramod Thupaki - automatically get the file name from path using os aware method
    ss = [os.path.basename(i).split(".")[0] for i in sub_set]
    gs = [os.path.basename(i).split(".")[0] for i in global_set]
    if opt == "not-in":
        list_ = [a not in ss for a in gs]
    elif opt == "in":
        list_ = [a in ss for a in gs]
    return [i for i in compress(global_set, list_)]
