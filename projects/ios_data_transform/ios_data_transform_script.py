import logging
import logging.config
import os
import sys
import glob
import argparse
from tqdm import tqdm
from multiprocessing import Process
from time import time
import cioos_data_transform.IosObsFile as ios
from write_ctd_ncfile import write_ctd_ncfile
from write_cur_ncfile import write_cur_ncfile
from write_mctd_ncfile import write_mctd_ncfile

# .cioos_data_transform as iod
import cioos_data_transform.utils as cioos_utils
import subprocess

log_config_path = os.path.join(os.path.dirname(__file__), "log_config.ini")
logging.config.fileConfig(log_config_path)
main_logger = logging.getLogger()
logger = logging.LoggerAdapter(main_logger, {"file": None})


def convert_files(config={}, opt="all", ftype=None):
    # path of raw files, path for nc files, and option
    # opt = 'new' for only new raw files
    # opt = 'all' for all files. default value;
    # ftype =   'ctd' for CTD profiles
    #           'mctd' for mooring CTDs
    #           'cur' for current meters
    logger.info("Option, option=%s, ftype =%s", opt, ftype)
    in_path = config.get("raw_folder")
    # out_path = config.get("nc_folder")
    # fgeo = config.get("geojson_file")
    if ftype == "ctd":
        flist = glob.glob(in_path + "**/*.[Cc][Tt][Dd]", recursive=True)
    elif ftype == "mctd":
        flist = []
        flist.extend(glob.glob(in_path + "**/*.[Cc][Tt][Dd]", recursive=True))
        flist.extend(glob.glob(in_path + "**/*.mctd", recursive=True))
    elif ftype == "bot":
        flist = []
        flist.extend(glob.glob(in_path + "**/*.[Bb][Oo][Tt]", recursive=True))
        flist.extend(glob.glob(in_path + "**/*.[Cc][Hh][Ee]", recursive=True))
    elif ftype == "cur":
        flist = glob.glob(in_path + "**/*.[Cc][Uu][Rr]", recursive=True)
    else:
        logger.error("ERROR: Filetype not understood ...")
        return None
    logger.info("Total number of files =%s", len(flist))
    # loop through files in list, read the data and write netcdf file if data read is successful
    for fname in tqdm(
        flist[:], unit="file", desc=f"Convert files {ftype} to netcdf format"
    ):
        # print('\nProcessing -> {} {}'.format(i, fname))
        logger.extra["file"] = fname
        p = Process(target=(convert_files_threads), args=(ftype, fname, config))
        p.start()
        p.join()
    return flist


def standardize_variable_names(ncfile):
    # input: netcdf file with non-standard variable names
    # output: netcdf file with standard variables added
    # NOTE: netcdf files are overwritten
    logger.info(f"Adding standard variables:{ncfile}")
    cioos_utils.add_standard_variables(ncfile)


def convert_files_threads(ftype, fname, config={}):
    # skip processing file if its older than 24 hours old
    if cioos_utils.file_mod_time(fname) < -24.0 and opt == "new":
        # print("Not converting file: ", fname)
        return 0
    logger.debug("Processing %s %s", ftype, fname)
    # read file based on file type
    if ftype == "ctd":
        fdata = ios.CtdFile(filename=fname, debug=False)
    elif ftype == "mctd":
        fdata = ios.MCtdFile(filename=fname, debug=False)
    elif ftype == "bot":
        fdata = ios.CtdFile(filename=fname, debug=False)
    elif ftype == "cur":
        fdata = ios.CurFile(filename=fname, debug=False)
    else:
        logger.error("Filetype not understood!")
        sys.exit()
    # if file class was created properly, try to import data
    if fdata.import_data():
        logger.debug("Imported data successfully!")
        fdata.assign_geo_code(config.get("geojson_file"))
        out_path = config.get("nc_folder")
        # now try to write the file...
        yy = fdata.start_date[0:4]
        if not os.path.exists(out_path + yy):
            os.makedirs(out_path + yy)
        ncFileName = out_path + yy + "/" + fname.split("/")[-1] + ".nc"
        if ftype == "ctd":
            try:
                write_ctd_ncfile(ncFileName, fdata, config=config)
                standardize_variable_names(ncFileName)
            except Exception as e:
                print("Error: Unable to create netcdf file:", fname, e)
                subprocess.call(["rm", "-f", ncFileName])
        elif ftype == "mctd":
            try:
                write_mctd_ncfile(ncFileName, fdata, config=config)
                standardize_variable_names(ncFileName)
            except Exception as e:
                print("Error: Unable to create netcdf file:", fname, e)
                subprocess.call(["rm", "-f", ncFileName])
        elif ftype == "bot":
            try:
                write_ctd_ncfile(ncFileName, fdata, config=config)
                standardize_variable_names(ncFileName)
            except Exception as e:
                print("Error: Unable to create netcdf file:", fname, e)
                subprocess.call(["rm", "-f", ncFileName])
        elif ftype == "cur":
            try:
                write_cur_ncfile(ncFileName, fdata, config=config)
            except Exception as e:
                print("Error: Unable to create netcdf file:", fname, e)
                subprocess.call(["rm", "-f", ncFileName])
    else:
        print("Error: Unable to import data from file", fname)
        return 0


# read inputs if any from the command line
# first input is 'all' or 'new' for processing all files or just files newer than 24 hours
# second input is file type and is one of ['ctd','mctd', 'cur', 'bot']
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="IOStoNetCDF",
        description="Convert IOS Shell file format to NetCDF format.",
        epilog="CIOOS (2023)",
    )
    parser.add_argument(
        "opt",
        default="all",
        choices=("all", "new"),
        help="all: processing all files or new: just files newer than 24 hours",
    )
    parser.add_argument("ftype", default="ctd", help="IOS file type to convert")
    args = parser.parse_args()
    opt, ftype = args.opt, args.ftype

    config = cioos_utils.read_config(f"config_{ftype}.json")
    logger.debug("Inputs from config file: %s", config)
    start = time()
    flist = convert_files(config=config, opt=opt, ftype=ftype)
    logger.info("Time taken to convert files: %0.02fs", time() - start)
    # if any raw files have been removed, delete corresponding netCDF files
    if flist is not None:
        logger.info("Checking if any netCDF files should be removed...")
        ncfilelist = glob.glob(config.get("nc_folder") + "**/*.nc", recursive=True)
        for e in cioos_utils.compare_file_list(sub_set=flist, global_set=ncfilelist):
            filename = glob.glob(config.get("nc_folder") + "**/{}.nc")
            logger.info("deleting file: %s", e)
            subprocess.call(["rm", "-f", e])
    logger.info("Total time taken %0.2fs", time() - start)
