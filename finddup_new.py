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
import time
import subprocess
import re
from functools import partial
import multiprocessing.pool
import tictoc

# how much total memory bytes to use during comparison of files (Larger is faster up to a point)
MEM_TO_USE = 512*1024*1024    # 512MB
MEM_TO_USE = 2*1024*1024*1024 # 2GB
MEM_TO_USE = 1024*1024*1024   # 1GB

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

    print("Analyzing files...", file=sys.stderr)

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


# get filestat on file if possible (i.e. readable), discard if symlink, pipe, fifo
# from filestat return filesize, file mod_time
# return (-1,-1) if discarded file
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


# filetree is dict of dicts and items
#   each subdir is a nested dict subtree containing dicts and items
#   base of filetree corresponds to master_root
# return valid pointer to subtree of tree corresponding to root dir
# base of dict tree is relative to string master_root
# root is string of dir to get dict of (also containing master_root string)
# create tree dict hierarchical structure if needed to get to root
def subtree_dict(filetree, root, master_root):
    # root includes master_root
    #print( "Item:" )
    #print( "  root: " + root)
    #print( "  master_root: " + master_root)
    root_relative = os.path.relpath(root, start=master_root)
    #print( "  root_relative to master_root: " + root_relative)
    subtree = filetree
    for pathpart in root_relative.split( os.path.sep ):
        if pathpart and pathpart != '.':
            subtree = subtree.setdefault(pathpart,{})
    return subtree


# go through every file hierarchically inside argument paths
# make hash with keys being filesize in bytes and value being list of files that match
def hash_files_by_size( paths, master_root ):
    filesizes = {}
    filetree = {}
    filemodtimes = {}
    filesreport_time = time.time()
    needs_cr = False
    filesdone = 0

    # local function to process one file
    def process_file_size():
        # read/write these from hash_files_by_size scope
        nonlocal filesdone, filesreport_time, needs_cr

        # set filename branch of filetree to -1 (placeholder, meaning no id)
        subtree_dict(filetree, root, master_root)[filename] = -1

        filepath = os.path.join(root,filename)
        (this_size, this_mod) = check_stat_file(filepath)
        if this_size == -1:
            return

        # setdefault returns [] if this_size key is not found
        # append as item to filesizes [filepath,filemodtime] to check if modified later
        filesizes.setdefault(this_size,[]).append(filepath)
        filemodtimes[filepath] = this_mod

        filesdone+=1
        if filesdone%1000 == 0 or time.time()-filesreport_time > 15:
            print( "\r  "+str(filesdone)+" files sized.", end='', file=sys.stderr, flush=True)
            filesreport_time = time.time()
            needs_cr = True

    # Actual hierarchical processing
    for treeroot in paths:
        if needs_cr:
            print("", file=sys.stderr)
            needs_cr = False
        print("Starting sizing of: "+treeroot, file=sys.stderr)
        # remove trailing slashes, etc.
        treeroot = os.path.normpath(treeroot)
        if os.path.isdir( treeroot ):
            for (root,dirs,files) in os.walk(treeroot):
                # TODO: get modtime on directories too, to see if they change?
                for filename in files:
                    process_file_size()
        else:
            # this treeroot was a file
            process_file_size()

        # print final tally with CR
        print("\r  "+str(filesdone)+" files sized.", file=sys.stderr)
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
    return (filesizes,uniquefiles,filemodtimes,filetree)


# datachunks_list: list of arrays
# match_groups: list of indicies_match_lists, indicies_match_list is all indicies with identical arrays
def matching_array_groups(datachunks_list):
    match_groups = []
    # copy into remaining chunks
    ungrp_chunk_indicies = range(len(datachunks_list))
   
    # loop through chunks, looking for matches in unsearched chunks for first item in unsearched chunks
    #   item will always match itself, may match others
    #   save all matching indicies for this chunk into list of indicies appended to match_groups
    while(ungrp_chunk_indicies): # e.g. while len > 0
        #print(ungrp_chunk_indicies)
        test_idx = ungrp_chunk_indicies[0]
        matching_indicies = [i for i in ungrp_chunk_indicies if datachunks_list[i]==datachunks_list[test_idx]]
        match_groups.append(matching_indicies)
        ungrp_chunk_indicies = [x for x in ungrp_chunk_indicies if x not in matching_indicies]

    return match_groups


def compare_file_group(filelist):
    #print('------ compare_file_group -----------')
    max_files_open = 1 # TODO: we can optimize by allowing multiple files open at the same time

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
    while(filelist_groups_next): # i.e. while len > 0
        filelist_groups = filelist_groups_next[:]
        # reset next groups
        filelist_groups_next = []

        # for debugging print current groups every time through
        #print([len(x) for x in filelist_groups])

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

            # for each group > 1 member, see if we need to keep searching it or got to end of files
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

        if filelist_groups_next: # i.e if non-empty
            # max file read is total memory to be used divided by num of files in largest group this iter
            # total no more than MEM_TO_USE
            max_file_read = MEM_TO_USE // max([len(x) for x in filelist_groups_next])
            #max_file_read = 5 # small for debugging
            if max_file_read < 5:
                raise Exception("compare_file_group: too many files to compare: "+str(len(filelist)))
            #print("max_file_read = "+str(max_file_read))
        
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

    return (dup_groups,unique_files,invalid_files)


def create_file_ids(dup_groups, unique_files, invalid_files, filetree, master_root):
    file_id = {}
    idnum = 0;
    for unq_file in unique_files:
        file_id[unq_file] = idnum

        # add id to tree
        (unq_dir,unq_file) = os.path.split(unq_file)
        subtree_dict(filetree, unq_dir, master_root)[unq_file] = idnum

        idnum += 1
    for dup_group in dup_groups:
        for dup_file in dup_group:
            file_id[dup_file] = idnum

            # add id to tree
            (dup_dir,dup_file) = os.path.split(dup_file)
            subtree_dict(filetree, dup_dir, master_root)[dup_file] = idnum
        idnum += 1

    return file_id


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


# remove redundant searchpaths
# find master_root common root for all searchpaths
def process_searchpaths( searchpaths ):
    remove_searchpath = {}

    # convert to absolute paths
    new_searchpaths = [os.path.abspath(x) for x in searchpaths ]
    # eliminate duplicate paths
    new_searchpaths = list(set(new_searchpaths))
    # search for paths that are subdir of another path, eliminate them
    for searchpath1 in new_searchpaths:
        for searchpath2 in new_searchpaths:
            test_relpath = os.path.relpath(searchpath1, start=searchpath2)
            if test_relpath != '.' and not test_relpath.startswith('..'):
                # if '.' : searchpath1 and searchpath2 are same path (search artifact)
                # if relpath doesn't start with .. , then searchpath1 is subdir of searchpath2
                remove_searchpath[searchpath1]=True
    for (i,searchpath) in enumerate(new_searchpaths):
        if remove_searchpath.get(searchpath,False):
            del(new_searchpaths[i])
        
    master_root = os.path.commonpath( new_searchpaths )
    return (master_root, new_searchpaths)


# IDEA
# when recursing filetree with os.walk and using fstat, for directories:
#   record dir fullpath (like file)
#   record dir mod_time
#   record dir size as sum of children dirs and files
#   record placeholder for dir of all children files and directories
# Later,
#   go back through directories, and update placeholder with file_id instead of names

def main(argv=None):
    mytimer = tictoc.Timer()
    mytimer2 = tictoc.Timer()
    mytimer2.start()
    args = process_command_line(argv)

    # NEW IDEA:
    #   1. For every file, get: size, mod_time
    #   2. hash by sizes, each hash size in dict fill with list of all files that size
    #   3. go through each hash, deciding which of the list are unique or same
    #       by comparing matching-size files block by block, splitting into subgroups as differences found

    (master_root, searchpaths) = process_searchpaths( args.searchpaths )
    print( "searchpaths: "+str(searchpaths))
    print( "master_root: "+master_root)
    
    # filesizes is dict: keys are file sizes in bytes, items are lists of filepaths that
    #   are all that size (lists are all len > 1)
    # unique_size_files is list: filepaths
    # filemodtimes is dict: keys are filepaths, items are modtimes when file was stat'ed
    (filesizes, unique_size_files, filemodtimes, filetree) = hash_files_by_size( searchpaths, master_root )

    # compare all filegroups by using byte-by-byte comparison to find actually unique, duplicate
    # dupgroups: list of lists of identical files
    # uniquefiles: list unique files
    # invalid_files: list problematic (mostly unreadable) files
    (dup_groups,unique_files,invalid_files) = compare_files(filesizes)

    # make sure all unique files are folded into list unique_files
    unique_files.extend(unique_size_files)

    # now we know all of the files that are duplicates and unique
    # we will also determine below which directories are identical in that they contain identical
    #   files and subdirs

    # for each unique file, make a unique id, identical-data files share id
    #   save unique id to file_id dict (key=filepath, item=id)
    #   save unique id to items of files in filetree hierarchical dict
    file_id = create_file_ids(dup_groups, unique_files, invalid_files, filetree, master_root)

    print(filetree)

    #all_hashes = filetree2hashes( filetree )

    #analyze_hashes( all_hashes )

    # find matching directories containing sets of matching files
    # also check if files have changed since start of analysis and denote them unknown

    # do we need a unique id for each file that is item for filepath key?
    #   then we could just go through tree, constructing dir key from contents keys
    #   any file that is invalid or unknown invalidates all dirs above it


    # final tally (DEBUG)
    print("unique_files:")
    print(unique_files)
    print("\n\ndup_groups:")
    for dup_group in dup_groups:
        print(dup_group)
    print("\n\ninvalid_files:")
    print(invalid_files)
    print("")

    mytimer2.eltime_pr("Elapsed time: ", prfile=sys.stderr )
    mytimer2.eltime_pr("Elapsed time: ", prfile=sys.stdout )
    return 0

if __name__ == '__main__':
    status = main(sys.argv)
    sys.exit(status)
