import sys
import os
sys.path.insert(0, os.getcwd()+'/../../')
import ios_data_transform as iod


def convert_mctd_files(f, out_path):
    fdata = iod.MCtdFile(filename=f, debug=True)
    if fdata.import_data():
        print(" <- Read data successfully ! ")
        iod.write_mctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)


# file = '/home/pramod/data/ios_mooring_data/mooring_data/AMP2-B/CTD/sogn_20081022_20081121_0049m.ctd'
file = '/home/pramod/data/ios_mooring_data/mooring_data/AMP2-B/CTD/sogs_20081022_20081119_0050m.ctd'
convert_mctd_files(f=file, out_path='/home/pramod/temp/')
