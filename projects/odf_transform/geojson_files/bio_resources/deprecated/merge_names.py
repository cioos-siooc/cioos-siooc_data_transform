import json

source_file = "odf_transform/geojson_files/Climate_ScotiaFundy_Polygons.geojson"
names_file = "odf_transform/test/Climate_ScotiaFundy_area_names.json"
destination_file = (
    "odf_transform/geojson_files/Climate_ScotiaFundy_Polygons_names.geojson"
)
match_on = "Area"
add_fields = ["name"]


with open(source_file) as file_obj:
    source = json.load(file_obj)

with open(names_file) as file_obj:
    names = json.load(file_obj)

for index, feature in enumerate(source["features"]):
    area_num = feature["properties"]["Area"]

    for area in names["scotian_shelf_areas"]:
        if area_num == area["Area"]:
            source["features"][index]["properties"]["name"] = area["name"]
            break


with open(destination_file, "w") as file_obj:
    json.dump(source, file_obj)