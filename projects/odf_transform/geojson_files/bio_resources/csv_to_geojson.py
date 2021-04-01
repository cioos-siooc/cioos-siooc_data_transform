import csv
import geojson

from geojson import Feature, FeatureCollection, Polygon

features = []

with open('climatePolygons.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    reader.__next__() # skip first line
    prev_area = ''
    poly_list = []
    for idx, area, lat, lon, region in reader:
        latitude, longitude = map(float, (lat, lon))
        if not prev_area:
            prev_area = area
        if prev_area == area:
            poly_list.append((longitude, latitude))
        else:
            # write out current polygon
            features.append(
                Feature(
                    geometry = Polygon(poly_list)
                )
            )
            # reset polygon coordinate list
            
        