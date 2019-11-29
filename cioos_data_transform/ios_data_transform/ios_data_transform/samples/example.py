import sys
import os
sys.path.insert(0, os.getcwd()+'/../../')
import ios_data_transform as iod


def convert_mctd_files(f, out_path):
    fdata = iod.MCtdFile(filename=f, debug=False)
    if fdata.import_data():
        fdata.assign_geo_code('../tests/test_files/ios_polygons.geojson')
        iod.write_mctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)


def convert_bot_files(f, out_path):
    fdata = iod.BotFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code('../tests/test_files/ios_polygons.geojson')
        iod.write_ctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)


def convert_ctd_files(f, out_path):
    fdata = iod.CtdFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code('../tests/test_files/ios_polygons.geojson')
        iod.write_ctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)

# convert_mctd_files(f='/home/pramod/data/ios_mooring_data/mooring_data/AMP2-B/CTD/sogn_20081022_20081121_0049m.ctd', out_path='/home/pramod/temp/')
# convert_mctd_files(f='/home/pramod/data/ios_mooring_data/mooring_data/AMP2-B/CTD/sogs_20081022_20081119_0050m.ctd', out_path='/home/pramod/temp/')
convert_ctd_files(f='/home/pramod/data/ios_mooring_data/cruise_data/2019/2019-001/CTD/2019-001-0001.ctd', out_path='/home/pramod/temp/')

convert_bot_files(f='/home/pramod/data/ios_mooring_data/cruise_data/1930/1930-31/BOTTLE/1930-31-0001.bot', out_path='/home/pramod/temp/')
# convert_bot_files(f='/home/pramod/data/ios_mooring_data/cruise_data/MEDS_Files/NCOASTX/6171/61710010.BOT', out_path='/home/pramod/temp/')
