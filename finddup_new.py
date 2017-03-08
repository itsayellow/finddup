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
import stat
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


def check_stat_file(filepath):
    # TODO: experimental
    if IGNORE_FILES.get(os.path.basename(filepath),False):
        return (-1,-1)

    # TODO: I think we want to skip links to files but need to confirm
    # skip symbolic links without commenting
    if os.path.islink(filepath):
        return (-1,-1)

    try:
        this_filestat = os.stat(filepath)
    except OSError as e:
        # e.g. FileNotFoundError, PermissionError
        if needs_cr:
            print("", file=sys.stderr)
            needs_cr = False
        print("Filestat Error opening: "+filepath, file=sys.stderr)
        print("  Error: "+str(type(e)), file=sys.stderr )
        print("  Error: "+str(e), file=sys.stderr )
        return (-1,-1)
    except:
        e = sys.exc_info()
        # TODO: use stderr, check and later set needs_cr
        if needs_cr:
            print("", file=sys.stderr)
            needs_cr = False
        print("Unhandled File Stat on: "+filepath, file=sys.stderr )
        print("  Error: "+str(e[0]), file=sys.stderr )
        print("  Error: "+str(e[1]), file=sys.stderr )
        print("  Error: "+str(e[2]), file=sys.stderr )
        return (-1,-1)

    # skip FIFOs without commenting
    if stat.S_ISFIFO(this_filestat.st_mode):
        return (-1,-1)
    # skip sockets without commenting
    if stat.S_ISSOCK(this_filestat.st_mode):
        return (-1,-1)

    this_size = this_filestat.st_size
    this_mod = this_filestat.st_mtime
    
    return (this_size, this_mod)


# go through every file and make hash with keys being filesize in bytes
#   and value being list of files that match
#     os.path.getsize('C:\\Python27\\Lib\\genericpath.py')
#      OR
#     os.stat('C:\\Python27\\Lib\\genericpath.py').st_size
def hash_files_by_size( paths ):
    filesizes = {}
    filemodtimes = {}
    filesreport_time = time.time()
    needs_cr = False
    for treeroot in paths:
        if needs_cr:
            print("", file=sys.stderr)
            needs_cr = False
        print("Starting sizing of: "+treeroot, file=sys.stderr)
        # remove trailing slashes, etc.
        treeroot = os.path.normpath(treeroot)
        if os.path.isdir( treeroot ):
            filesdone = 0
            for (root,dirs,files) in os.walk(treeroot):
                for filename in files:
                    filepath = os.path.join(root,filename)
                    (this_size, this_mod) = check_stat_file(filepath)
                    if this_size == -1:
                        continue

                    # setdefault returns [] if this_size key is not found
                    # append as item to filesizes [filepath,filemodtime] to check if modified later
                    filesizes.setdefault(this_size,[]).append(filepath)
                    filemodtimes[filepath] = this_mod

                    filesdone+=1
                    if filesdone%1000 == 0 or time.time()-filesreport_time > 15:
                        print(
                                "\b"*40+"  "+str(filesdone)+" files sized.",
                                end='', file=sys.stderr, flush=True
                                )
                        filesreport_time = time.time()
                        needs_cr = True
        else:
            # this treeroot was a file
            filepath = treeroot
            (this_size, this_mod) = check_stat_file(filepath)
            if this_size == -1:
                continue

            # setdefault returns [] if this_size key is not found
            # append as item to filesizes [filepath,filemodtime] to check if modified later
            filesizes.setdefault(this_size,[]).append(filepath)
            filemodtimes[filepath] = this_mod

            filesdone+=1
            if filesdone%1000 == 0 or time.time()-filesreport_time > 15:
                print(
                        "\b"*40+"  "+str(filesdone)+" files sized.",
                        end='', file=sys.stderr, flush=True
                        )
                filesreport_time = time.time()
                needs_cr = True

    # print final tally with CR
    print("\b"*40+"  "+str(filesdone)+" files sized.", file=sys.stderr)
    needs_cr = False

    unique = 0
    nonunique = 0
    uniquefiles = []
    for key in filesizes.keys():
        if len(filesizes[key])==1:
            unique += 1
            uniquefiles.append(filesizes[key][0])
        else:
            nonunique += len(filesizes[key])

    # remove all unique filesize files from filesizes
    filesizes = {k:filesizes[k] for k in filesizes if len(filesizes[k])>1}

    print("\nUnique: %d    "%unique, file=sys.stderr)
    print("Possibly Non-Unique: %d\n"%nonunique, file=sys.stderr)
    return (filesizes,uniquefiles,filemodtimes)


# datachunks_list: list of arrays
# match_groups: list of indicies_match_lists, indicies_match_list is all indicies with identical arrays
def matching_array_groups(datachunks_list):
    match_groups = []
    # copy into remaining chunks
    ungrp_chunk_indicies = range(len(datachunks_list))
   
    # loop through chunks, looking for matches in unsearched chunks for first item in unsearched chunks
    #   item will always match itself, may match others
    #   save all matching indicies for this chunk into list of indicies appended to match_groups
    while(len(ungrp_chunk_indicies) > 0):
        #print(ungrp_chunk_indicies)
        test_idx = ungrp_chunk_indicies[0]
        matching_indicies = [i for i in ungrp_chunk_indicies if datachunks_list[i]==datachunks_list[test_idx]]
        match_groups.append(matching_indicies)
        ungrp_chunk_indicies = [x for x in ungrp_chunk_indicies if x not in matching_indicies]

    return match_groups


def compare_file_group(filelist):
    # max file read is total memory to be used divided by len(filelist)
    # total no more than 512MB, each no more than 1MB
    max_file_read = 512*1024*1024 // len(filelist)
    max_file_read = min(1024*1024,max_file_read)
    #max_file_read = 5 # small for debugging
    if max_file_read < 5:
        raise Exception("compare_file_group: too many files to compare: "+str(len(filelist)))
    print("max_file_read = "+str(max_file_read))
    
    max_files_open = 1 # later we can optimize by allowing multiple files open at the same time

    # amt_file_read starts small on first pass (most files will be caught)
    #   later it will be upped to max_file_read for next passes
    amt_file_read = 256

    # init empty lists to append to
    unique_files = []
    dup_groups = []
    invalid_files = []

    # right now only one prospective group of files, split later if distinct file groups are found
    filelist_groups_next=[filelist]

    # initial file position is 0
    filepos = 0

    # TODO: if total files is small enough  (< 100?), keep them all open while reading, close when they are 
    #       called unique or dup  (Or at very end!)
    while(len(filelist_groups_next)>0):
        filelist_groups = filelist_groups_next[:]
        # reset next groups
        filelist_groups_next = []

        # for debugging print current groups every time through
        print([len(x) for x in filelist_groups])

        # each filelist_group is a possible set of duplicate files
        # a file is split off from a filelist_group as it is shown to be different from others in group,
        #   either to a subgroup of matching files or by itself
        for filelist_group in filelist_groups:
            filedata_list = []
            filedata_size_list = []
            # open files one at a time and close after getting each file's data into filedata_list
            for thisfile in filelist_group:
                try:
                    with open(thisfile,'rb') as thisfile_fh:
                        thisfile_fh.seek(filepos)
                        this_filedata = thisfile_fh.read(amt_file_read)
                    filedata_list.append(this_filedata)
                    # get how many bytes we actually read (may be less than max)
                    filedata_size_list.append(len(this_filedata))
                #except (FileNotFoundError, PermissionError) as e:
                except OSError as e:
                    # e.g. FileNotFoundError, PermissionError
                    print("Error opening: "+thisfile)
                    print("  Error: "+str(type(e)) )
                    print("  Error: "+str(e) )
                    invalid_files.append([ thisfile, str(type(e)), str(e) ])
                    # append -1 to signify invalid
                    filedata_list.append(-1)
                    filedata_size_list.append(-1)
                except:
                    e = sys.exc_info()
                    print("UNHANDLED Error opening: "+thisfile)
                    print("  Error: "+str(e[0]))
                    print("  Error: "+str(e[1]))
                    print("  Error: "+str(e[2]))
                    raise e[0]

                    #invalid_files.append([ thisfile, str(e[0]), str(e[1]) ])
                    ## append -1 to signify invalid
                    #filedata_list.append(-1)
                    #filedata_size_list.append(-1)

            # remove invalid files from filelist_group, filedata_list, filedata_size_list
            invalid_idxs = [i for i in range(len(filedata_size_list)) if filedata_size_list[i]==-1]
            filelist_group = [x for (i,x) in enumerate(filelist_group) if i not in invalid_idxs]
            filedata_list= [x for (i,x) in enumerate(filedata_list) if i not in invalid_idxs]
            filedata_size_list= [x for (i,x) in enumerate(filedata_size_list) if i not in invalid_idxs]

            # get groups of indicies with datachunks that match each other
            match_idx_groups = matching_array_groups(filedata_list)

            single_idx_groups = [ x for x in match_idx_groups if len(x)==1 ]
            match_idx_groups = [ x for x in match_idx_groups if x not in single_idx_groups ]

            unique_files.extend([ filelist_group[sing_idx_group[0]] for sing_idx_group in single_idx_groups ])

            # we stop reading a file if it is confirmed unique, or if we get to the end of the file

            for match_idx_group in match_idx_groups:
                filedata_sizes_group = [filedata_size_list[i] for i in match_idx_group]
                # all filedata_sizes in matching group should be equal, so check the first one as representve
                if filedata_sizes_group[0] < amt_file_read:
                    # if entire matching group has less data than we tried to read, we are at end of files
                    #   and this is a final dupgroup
                    dup_groups.append([filelist_group[i] for i in match_idx_group])
                else:
                    # if filedata size is amt_file_read then not at end of files, keep reading / checking
                    filelist_groups_next.append([filelist_group[i] for i in match_idx_group])

        # increment file position for reading next time through groups
        filepos = filepos + amt_file_read
        # after first pass dramatically increase file reads
        amt_file_read = max_file_read

    return (unique_files,dup_groups,invalid_files)


def compare_files(filesizes):
    unique_files = []
    dup_groups = []
    invalid_files = []

    for key in filesizes.keys():
        (this_unique_files,this_dup_groups,this_invalid_files) = compare_file_group(filesizes[key])
        unique_files.extend(this_unique_files)
        dup_groups.extend(this_dup_groups)
        invalid_files.extend(this_invalid_files)

    print("unique_files:")
    print(unique_files)

    print("\n\ndup_groups:")
    for dup_group in dup_groups:
        print(dup_group)
    print("\n\ninvalid_files:")
    print(invalid_files)
    print("")

    return (dup_groups,unique_files,invalid_files)


def main(argv=None):
    mytimer = tictoc.Timer()
    mytimer2 = tictoc.Timer()
    mytimer2.start()
    args = process_command_line(argv)

    # NEW IDEA:
    #   1. For every file, get: size, mod_time
    #   2. hash by sizes, each hash size in dict fill with list of all files that size
    #   3. go through all hashes, for each hash deciding which of the list are unique or same

    # filesizes is dict: keys are file sizes in bytes, items are lists of 2-tuples (filepath,modtime) that
    #   are all that size
    # unique_size_files is dict: keys are filepath, items are sizes (TODO: maybe not useful??)
    (filesizes, unique_size_files, filemodtimes) = hash_files_by_size( args.searchpaths )

    # TODO: filesizes hash also contains verified unique files, we need to omit them from compare_files
    (dupgroups,uniquefiles,invalid_files) = compare_files(filesizes)

    # testing
    mytimer.start()
    #all_files_hashes = get_all_hashes( args.searchpaths, uniquefiles, args.verbose )
    mytimer.eltime_pr("get_all_hashes: ", prfile=sys.stderr )
    #print( "all_files_hashes" )
    #print( all_files_hashes )

    #filetree = make_hashes( args.searchpaths, all_files_hashes, uniquefiles, args.verbose )

    #all_hashes = filetree2hashes( filetree )

    #print_hashes( all_hashes )

    #analyze_hashes( all_hashes )

    mytimer2.eltime_pr("Elapsed time: ", prfile=sys.stderr )
    mytimer2.eltime_pr("Elapsed time: ", prfile=sys.stdout )
    return 0

if __name__ == '__main__':
    status = main(sys.argv)
    sys.exit(status)
