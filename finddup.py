#!/usr/bin/env python3
#
# finddup - find duplicate files even if they have different names
#               searching throughout all paths

# TODO: ignore files, like .DS_Store that don't really affect matching
#       (maybe in .finddup/ignore ? )
# TODO: nice to know if a directory contains only matching files, even if that
#   directory doesn't match another directory completely
#     e.g. DIR1: fileA, fileB
#          DIR2: fileA, fileB, fileC
#     still might want to delete DIR1 even though it doesn't match exactly DIR2
import os
import os.path
import sys
import argparse
import hashlib
import time
import subprocess
import re
from functools import partial
import multiprocessing.pool
import tictoc

#  dir2/file3
#  dir2/dir1
#  dir2/dir1/file1
#  dir2/dir1/file2
#file1 = 0923
#file2 = 3492
#file3 = 7103
#dir1 = [09233492]
#dir2 = [7103[09233492]]
#>>> a['dir2']['dir1']['file1']=923
#>>> a['dir2']['dir1']['file1']='0923'
#>>> a['dir2']['dir1']['file2']='3492'
#>>> a['dir2']['file3']='7103'
#>>> a
#{'dir2': {'file3': '7103', 'dir1': {'file1': '0923', 'file2': '3492'}}}
#
# return_hash('dir2/dir1')['file1'] - '0923'

# HACK! TODO
IGNORE_FILES = {".picasa.ini":True,".DS_Store":True,"Thumbs.db":True,"Icon\r":True}

def process_command_line(argv):
    """
    Return a 2-tuple: (settings object, args list).
    `argv` is a list of arguments, or `None` for ``sys.argv[1:]``.
    """
    script_name = argv[0]
    argv = argv[1:]

    # initialize the parser object:
    parser = argparse.ArgumentParser(
            description="Find duplicate files in all paths.  Looks at file content, not names or info." )

    # specifying nargs= puts outputs of parser in list (even if nargs=1)

    # required arguments
    parser.add_argument( 'searchpaths', nargs='+',
            help = "Search path(s) (recursively searched)."
            )

    # switches/options:
    parser.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='Verbose status messages.' )

    #(settings, args) = parser.parse_args(argv)
    args = parser.parse_args(argv)

    return args

# TODO: hangs trying to open
#   ~/Library/Application Support/LastPass/pipes/lastpassffplugin
#   TODO: don't open pipes (use os.path.isfile() ??)
def hash_file( filepath, am_verbose ):
    hasher = hashlib.md5()   # 55sec/682 images
    #hasher = hashlib.sha256() # 62sec/682 images
    blocksize = 65536;

    # only look at regular files that are not symbolic links
    if os.path.isfile( filepath ) and not os.path.islink( filepath ):
        try:
            fp = open( filepath, 'rb')
        except OSError:
            #sys.stderr.write("Can't read: "+filepath+"\n")
            print("Can't read: "+filepath)
            return "-1"
        #sys.stderr.write("Reading "+filepath+"\n")
        try:
            buf = fp.read(blocksize)
        except OSError:
            fp.close()
            #sys.stderr.write("Problem reading: "+filepath+"\n")
            print("Problem reading: "+filepath)
            return "-1"
        while len(buf) > 0:
            hasher.update(buf)
            buf = fp.read(blocksize)
        fp.close()
        # TODO: return hex instead for compactness
        return hasher.hexdigest()
    else:
        #sys.stderr.write("Not a regular file: "+filepath+"\n")
        if am_verbose:
            print("Not a regular file: "+filepath)
        return "-1"


# TODO: hangs trying to open
#   ~/Library/Application Support/LastPass/pipes/lastpassffplugin
#   TODO: don't open pipes (use os.path.isfile() ??)
def hash_file_map( filepath, am_verbose ):
    hasher = hashlib.md5()   # 55sec/682 images
    #hasher = hashlib.sha256() # 62sec/682 images
    blocksize = 65536;

    # only look at regular files that are not symbolic links
    if os.path.isfile( filepath ) and not os.path.islink( filepath ):
        try:
            fp = open( filepath, 'rb')
        except OSError:
            #sys.stderr.write("Can't read: "+filepath+"\n")
            print("Can't read: "+filepath)
            this_hash = "-1"
        else:
            #sys.stderr.write("Reading "+filepath+"\n")
            try:
                buf = fp.read(blocksize)
            except OSError:
                fp.close()
                #sys.stderr.write("Problem reading: "+filepath+"\n")
                print("Problem reading: "+filepath)
                this_hash = "-1"
            else:
                while len(buf) > 0:
                    hasher.update(buf)
                    buf = fp.read(blocksize)
                fp.close()
                # TODO: return hex instead for compactness
                this_hash = hasher.hexdigest()
    else:
        #sys.stderr.write("Not a regular file: "+filepath+"\n")
        if am_verbose:
            print("Not a regular file: "+filepath)
        this_hash = "-1"

    return (filepath, this_hash)

# recurse
# filetree is a dict of dicts
# treeroot is the top of a tree (don't split it further)
# root is current root (some of which is treeroot)
# return a dict
def return_dict(filetree,treeroot,root):
    if treeroot == root:
        # if we've reached the root of the tree, then recursion stops, return
        #   the dict
        # if dict doesn't exist, then return empty dict
        if treeroot not in filetree:
            filetree[treeroot] = {}
        return filetree[treeroot]
    else:
        (root1,root2) = os.path.split(root)
        filetree_branch = return_dict( filetree, treeroot, root1)
        if root2 not in filetree_branch:
            filetree_branch[root2]={}
        return filetree_branch[root2]


# input paths is list of paths to be searched recursively
# input uniquefiles are files that have unique size and don't need to be hashed
# input am_verbose is option to print more messages
# output all_hashes is dict, keys are full filepaths, items are hash values
def get_all_hashes( paths, uniquefiles, am_verbose ):
    all_hashes = {}
    files_tohash = []

    # first get list of all files that need to be hashed
    for treeroot in paths:
        if os.path.isdir( treeroot ):
            for (root,dirs,files) in os.walk(treeroot):
                for filename in files:
                    if IGNORE_FILES.get(filename,False):
                        continue
                    filepath = os.path.join(root,filename)
                    if filepath in uniquefiles:
                        # guaranteed not to match hash_file return of pure hex
                        all_hashes[filepath] = "size:"+str(uniquefiles[filepath])
                    else:
                        files_tohash.append(filepath)
        else:
            print( "skipping file (TODO) "+treeroot)

    # multiprocessing speeds up by about x2 - x3 for 8-processor
    #   somewhat file access limited rather than processor limited
    with multiprocessing.Pool() as filehash_pool:
        files_hashes = filehash_pool.map(
                partial(
                    hash_file_map,
                    am_verbose=am_verbose
                    ),
                files_tohash
                )

    all_hashes.update( dict(files_hashes) )
    return all_hashes


def make_hashes( paths, all_hashes, uniquefiles, am_verbose ):
    filetree={}
    filesreport_time = time.time()
    needs_cr = False
    for treeroot in paths:
        if needs_cr:
            sys.stderr.write("\n")
            needs_cr = False
        sys.stderr.write("Starting hashing of: "+treeroot+"\n")
        # remove trailing slashes, etc.
        treeroot = os.path.normpath(treeroot)
        if os.path.isdir( treeroot ):
            filesdone = 0
            for (root,dirs,files) in os.walk(treeroot):
                for filename in files:
                    # TODO: experiemental
                    if IGNORE_FILES.get(filename,False):
                        continue
                    filepath = os.path.join(root,filename)
                    # TODO: the following has thrown a KeyError, why, how?
                    #   did the file get created since we made all_hashes?
                    this_hash = all_hashes[filepath]
                    filesdone+=1
                    if this_hash != "-1":
                        return_dict(filetree,treeroot,root)[filename]=this_hash
                    else:
                        if am_verbose:
                            print( "Not adding to dict: "+filename+this_hash)
                    if filesdone%100 == 0 or time.time()-filesreport_time > 15:
                        sys.stderr.write("\b"*40+"  "+str(filesdone)+" files hashed.")
                        sys.stderr.flush() #py3 doesn't seem to flush until \n
                        needs_cr = True
                        filesreport_time = time.time()
            sys.stderr.write("\b"*40+"  "+str(filesdone)+" files hashed.\n")
            needs_cr = False
        else:
            print( "skipping file (TODO) "+treeroot)

    return filetree


# go through every file and make hash with keys being filesize in bytes
#   and value being list of files that match
#     os.path.getsize('C:\\Python27\\Lib\\genericpath.py')
#      OR
#     os.stat('C:\\Python27\\Lib\\genericpath.py').st_size
def find_filesizes( paths ):
    filesizes = {}
    filesreport_time = time.time()
    needs_cr = False
    for treeroot in paths:
        if needs_cr:
            sys.stderr.write("\n")
            needs_cr = False
        sys.stderr.write("Starting sizing of: "+treeroot+"\n")
        # remove trailing slashes, etc.
        treeroot = os.path.normpath(treeroot)
        if os.path.isdir( treeroot ):
            filesdone = 0
            for (root,dirs,files) in os.walk(treeroot):
                for filename in files:
                    # TODO: experiemental
                    if IGNORE_FILES.get(filename,False):
                        continue
                    filepath = os.path.join(root,filename)
                    try:
                        this_size = os.path.getsize(filepath)
                    except:
                        this_size = -1;
                    if this_size != "-1":
                        # setdefault returns [] if this_size key is not found
                        filesizes.setdefault(this_size,[]).append(filepath)
                    else:
                        print( "Not adding to dict: "+filename+this_size)
                    filesdone+=1
                    if filesdone%1000 == 0 or time.time()-filesreport_time > 15:
                        sys.stderr.write("\b"*40+"  "+str(filesdone)+" files sized.")
                        sys.stderr.flush() #py3 doesn't seem to flush until \n
                        filesreport_time = time.time()
                        needs_cr = True
            sys.stderr.write("\b"*40+"  "+str(filesdone)+" files sized.\n")
            needs_cr = False
        else:
            print( "skipping file (TODO) "+treeroot)

    unique = 0
    nonunique = 0
    uniquefiles = {}
    for key in filesizes.keys():
        if len(filesizes[key])==1:
            unique += 1
            uniquefiles[filesizes[key][0]] = key
        else:
            nonunique += len(filesizes[key])
    sys.stderr.write("\nUnique: %d    "%unique)
    sys.stderr.write("Possibly Non-Unique: %d\n\n"%nonunique)
    return (filesizes,uniquefiles)


# if is file (hex string) add hex string to db, then return string
# if is dir (dict): sort keys, concatenate all
def recurse_filetree( filetree, all_hashes, path_parent ):
    filetree_str = ""
    for filedir in sorted(filetree.keys()):
        if type(filetree[filedir]) is dict:
            this_str = "["
            this_str += recurse_filetree(
                    filetree[filedir],
                    all_hashes,
                    os.path.join(path_parent,filedir)
                    )
            this_str += "]"
            # represents a directory, return [hashes]
            all_hashes[this_str] = all_hashes.get(this_str,[]) + [os.path.join(path_parent,filedir)+os.path.sep,]
        else:
            # represents a file, return hash
            this_str = filetree[filedir]
            all_hashes[this_str] = all_hashes.get(this_str,[]) + [os.path.join(path_parent,filedir),]
        filetree_str += this_str
    return filetree_str

# filetree is a dict of all files or directories at this level.  Each key
#   resolves to either a hex string or another dict (for a directory)
# e.g. subtree = filetree['.']
#   subtree['readme.txt']=9384920fa8d8x
#   subsubtree = subtree['subdir']
def filetree2hashes( filetree ):
    all_hashes = {}

    # this filetree_str is bogus: as if all cmdline dirs were in same dir
    filetree_str = recurse_filetree( filetree, all_hashes, "")
    return all_hashes

def print_hashes( all_hashes ):
    for hash in all_hashes.keys():
        print( hash )
        print( all_hashes[hash] )

def k2prefix( size_kb ):
    if size_kb > 1024*1024:
        return "%.3fGB"%(size_kb/1024/1024)
    elif size_kb > 1024:
        return "%.3fMB"%(size_kb/1024)
    else:
        return "%.fkB"%(size_kb)

def analyze_hashes( all_hashes ):
    unique_files = []
    dup_files = []

    sys.stderr.write("Analyzing files...\n")

    for key in all_hashes.keys():
        if len(all_hashes[key]) == 1:
            unique_files.extend(all_hashes[key])
        else:
            filedir = all_hashes[key][0]
            if os.path.isfile(filedir):
                # do it the quick way if we are a regular file
                #   off from du by at most 4k
                dup_size = int( os.path.getsize(filedir) / 1024.0 )
            else:
                dup_size = subprocess.check_output(["du","-sk",filedir])
                dup_size = dup_size.decode("utf-8")
                dup_size = int(re.search(r'\d+',dup_size).group(0))
            dup_files.append([dup_size,]+all_hashes[key])

    dup_files.sort(reverse=True, key=lambda dup_list: dup_list[0])
    unique_files.sort()

    print( "Duplicate Files/Directories:" )
    for filelist in dup_files:
        print( "Duplicate set (%s each)"%k2prefix(filelist[0]) )
        for filename in filelist[1:]:
            print( "  "+filename )

    print( "\n" )
    print( "Unique Files/Directories:" )
    for filename in unique_files:
        print( filename )

def main(argv=None):
    mytimer = tictoc.Timer()
    mytimer2 = tictoc.Timer()
    mytimer2.start()
    args = process_command_line(argv)

    (filesizes,uniquefiles) = find_filesizes( args.searchpaths )

    # testing
    mytimer.start()
    all_files_hashes = get_all_hashes( args.searchpaths, uniquefiles, args.verbose )
    mytimer.eltime_pr("get_all_hashes: ", prfile=sys.stderr )
    #print( "all_files_hashes" )
    #print( all_files_hashes )

    filetree = make_hashes( args.searchpaths, all_files_hashes, uniquefiles, args.verbose )

    all_hashes = filetree2hashes( filetree )

    #print_hashes( all_hashes )

    analyze_hashes( all_hashes )

    mytimer2.eltime_pr("Elapsed time: ", prfile=sys.stderr )
    mytimer2.eltime_pr("Elapsed time: ", prfile=sys.stdout )
    return 0

if __name__ == '__main__':
    status = main(sys.argv)
    sys.exit(status)
