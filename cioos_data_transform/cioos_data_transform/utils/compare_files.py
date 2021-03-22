import hashlib
import os
import json
import glob


def get_file_stats(flist):
    info = {}
    for f in flist:
        info[f] = [os.stat(f).st_size, int(os.stat(f).st_mtime / 60)]
    return info


def find_new_files(dir1):
    # Module to compare the file size and time stamp to find
    # files that have been modified/deleted/added

    # check if we have history for that directory
    finfo = "./.dir_stats"
    if os.path.exists(finfo):
        with open(finfo, "r") as fid:
            fdata = json.load(fid)
            old_dir_info = fdata.get(dir1)
        print("from file = ", len(fdata.keys()), len(old_dir_info.keys()))
    else:
        old_dir_info = {}
        fdata = {}

    flist = glob.glob(dir1, recursive=True)
    new_dir_info = get_file_stats(flist)
    # compare the two lists
    s1 = set(old_dir_info.keys())
    s2 = set(new_dir_info.keys())
    print("new info = ", len(s2))
    print("files deleted from folder =", s1 - s2)
    print("file in current folder newly added = ", s2 - s1)
    l2 = sorted(s1 & s2)
    change_log = []
    # go through file list and compare with old size and timestamp
    for f in l2:
        if not old_dir_info[f][1] == new_dir_info[f][1]:
            change_log.append(f)
            # print(f"{f} file has changed", dir_info_new[f][1], dir_info[f][1])
    print("number of files changed", len(change_log))
    # update information for that directory in the dictionary
    fdata[dir1] = new_dir_info
    with open(finfo, "w") as fid:
        print("Writing new file stats info to json file...")
        json.dump(fdata, fid)

    return change_log


def hash_file(fname):
    nBytes = 8192
    with open(fname, "rb") as f:
        md5 = hashlib.blake2b()
        chunk = f.read(nBytes)
        while chunk:
            md5.update(chunk)
            chunk = f.read(nBytes)
    return md5.hexdigest()


def compare_files(file1, file2, method="cmp"):
    if method == "cmp":
        return os.stat(file1) == os.stat(file2)
    elif method == "md5":
        return hash_file(file1) == hash_file(file2)


if __name__ == "__main__":
    # f = "/home/pramod/Downloads/ubuntu-20.04.2.0-desktop-amd64.iso"
    # print(compare_files(f, "./utils.py", method="cmp"))
    find_new_files("/home/pramod/data/IOS_Drifter_Data/*/*.drf")
