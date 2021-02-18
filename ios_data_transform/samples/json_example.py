import sys
import os
sys.path.insert(0, os.getcwd()+'/../../')
import ios_data_transform as iod
from shapely.geometry import Point


def convert_mctd_files(f, out_path):
    fdata = iod.MCtdFile(filename=f, debug=False)
    if fdata.import_data():
        print(" <- Read data successfully ! ")
        iod.write_mctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)


file = '../tests/test_files/ios_polygons.geojson'
polygons_dict = iod.utils.read_geojson(file)
print(iod.utils.find_geographic_area(polygons_dict, Point(-135.351, 49.124)))
