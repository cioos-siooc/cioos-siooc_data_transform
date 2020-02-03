import sys
import os
sys.path.insert(0, os.getcwd()+'/../../')
import ios_data_transform as iod


def convert_mctd_files(f, out_path):
    fdata = iod.MCtdFile(filename=f, debug=False)
    if fdata.import_data():
        fdata.assign_geo_code('../tests/test_files/ios_polygons.geojson')
        iod.write_mctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)
    else:
        print("Unable to import data from file", fdata.filename)


def convert_bot_files(f, out_path):
    fdata = iod.BotFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code('../tests/test_files/ios_polygons.geojson')
        iod.write_ctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)
    else:
        print("Unable to import data from file", fdata.filename)


def convert_ctd_files(f, out_path):
    fdata = iod.CtdFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code('../tests/test_files/ios_polygons.geojson')
        iod.write_ctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)
    else:
        print("Unable to import data from file", fdata.filename)

convert_mctd_files(f='/home/pramod/data/ios_mooring_data/mooring_data/AMP2-B/CTD/sogn_20081022_20081121_0049m.ctd', out_path='/home/pramod/temp/')
# convert_mctd_files(f='/home/pramod/data/ios_mooring_data/mooring_data/Aquaculture18/CTD/millar1_20171006_20181011_0017m_L1.ctd', out_path='/home/pramod/temp/')
# convert_ctd_files(f='/home/pramod/data/ios_mooring_data/cruise_data/2019/2019-001/CTD/2019-001-0001.ctd', out_path='/home/pramod/temp/')
# convert_ctd_files(f='/home/pramod/data/ios_mooring_data/cruise_data/1987/1987-73/CTD/1987-73-6206.CTD', out_path='/home/pramod/temp/')

# convert_ctd_files(f='/home/pramod/Downloads/1978-070-tn08.ctd', out_path='/home/pramod/temp/')
# convert_bot_files(f='/home/pramod/data/ios_mooring_data/cruise_data/1930/1930-31/BOTTLE/1930-31-0001.bot', out_path='/home/pramod/temp/')
# convert_bot_files(f='/home/pramod/data/ios_mooring_data/cruise_data/1933/1933-01/BOTTLE/1933-01-0004.BOT', out_path='/home/pramod/temp/')
# convert_bot_files(f='/home/pramod/data/ios_mooring_data/cruise_data/2016/2016-09/Bottle/2016-09-0003.bot', out_path='/home/pramod/temp/')
# convert_bot_files(f='/home/pramod/data/ios_mooring_data/cruise_data/1998/1998-29/BOTTLE/1998-29-8005.bot', out_path='/home/pramod/temp/')
convert_bot_files(f='/home/pramod/Downloads/1959-003-0017.bot', out_path='/home/pramod/temp/')
# convert_bot_files(f='/home/pramod/Downloads/1959-013-0001.bot', out_path='/home/pramod/temp/')
# convert_bot_files(f='/home/pramod/Downloads/1974-008-0015.bot', out_path='/home/pramod/temp/')