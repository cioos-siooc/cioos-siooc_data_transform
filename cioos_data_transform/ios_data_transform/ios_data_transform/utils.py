# general utility functions common to multiple classes

def is_in(keywords, string):
    # simple function to check if any keyword is in string
    # convert string and keywords to upper case before checking
    return any([string.upper().find(z.upper()) >= 0 for z in keywords])

def import_env_variables(filename='./.env'):
    # import information in file to a dictionary
    # this file makes the implementation independent of local folder
    # structure
    # data in file should be key:value pairs. Key should be unique
    info = {}
    with open(filename, 'r') as fid:
        lines = fid.readlines()
        for line in lines:
            info[line.split(':')[0].strip()] = line.split(':')[1].strip()
    return info

def file_mod_time(filename):
    # returns how old the file is based on timestamp
    # returns the time in hours
    import time
    import os
    dthrs = (os.path.getmtime(filename) - time.time())/3600.
    return dthrs
