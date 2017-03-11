#!/usr/bin/env python3
#
# finddup - find duplicate files, dirs even if they have different names
#               searching throughout all paths

# TODO: We need to have two classes of problem files: 1.) ignored, don't
#   matter for dir compare and 2.) read error, cause dir compare to be
#   unknown
# TODO: asterisk dirs that are dups if they contain ignored files
# TODO: double-check which files have changed during the preceding by
#   stating modtime on all files all over again, put changed files in
#   "unknown" category?
# TODO: for filegroups that have few filemembers, keep all open at same
#   time?
# TODO: nice to know if a directory contains only matching files, even if that
#   directory doesn't match another directory completely
#     e.g. DIR1: fileA, fileB
#          DIR2: fileA, fileB, fileC
#     still might want to delete DIR1 even though it doesn't match exactly DIR2
# TODO: could check if duplicate files have same inode? (hard link)?
#   maybe too esoteric

import os
import stat
import os.path
import sys
import argparse
import time
import textwrap
#import subprocess
#import re
#from functools import partial
#import multiprocessing.pool
import tictoc


# how much total memory bytes to use during comparison of files (Larger is faster up to a point)
MEM_TO_USE = 512*1024*1024    # 512MB
MEM_TO_USE = 2*1024*1024*1024 # 2GB
MEM_TO_USE = 1024*1024*1024   # 1GB


# HACK! TODO
IGNORE_FILES = {".picasa.ini":True,".DS_Store":True,"Thumbs.db":True,"Icon\r":True}


class StderrPrinter(object):
    def __init__(self):
        self.need_cr = False

    def print(self, text, **prkwargs):
        if text.startswith('\r'):
            self.need_cr = False
        if self.need_cr == True:
            print("", file=sys.stderr)

        print(text, file=sys.stderr, **prkwargs)

        if prkwargs.get('end','\n')=='' and not text.endswith('\n'):
            self.need_cr = True
        else:
            self.need_cr = False


# Global
myerr = StderrPrinter()


def process_command_line(argv):
    """
    Return a 2-tuple: (settings object, args list).
    `argv` is a list of arguments, or `None` for ``sys.argv[1:]``.
    """
    script_name = argv[0]
    argv = argv[1:]

    # initialize the parser object:
    parser = argparse.ArgumentParser(
            description="Find duplicate files and directories in all paths.  Looks at file content, not names or info." )

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


# copied from durank
# convert to string with units
#   use k=1024 for binary (e.g. kB)
#   use k=1000 for non-binary kW
def size2eng(size,k=1024):
    if   size > k**5:
        sizestr = "%.1fP" % (float(size)/k**5)
    elif size > k**4:
        sizestr = "%.1fT" % (float(size)/k**4)
    elif size > k**3:
        sizestr = "%.1fG" % (float(size)/k**3)
    elif size > k**2:
        sizestr = "%.1fM" % (float(size)/k**2)
    elif size > k:
        sizestr = "%.1fk" % (float(size)/k)
    else:
        sizestr = "%.1g" % (float(size))
    return sizestr


# get filestat on file if possible (i.e. readable), discard if symlink, pipe,
#   fifo
# from filestat return filesize, file mod_time, file blocks
# return (-1,-1,-1) if discarded file
def check_stat_file(filepath):
    extra_info = []

    try:
        # don't follow symlinks, just treat them like a regular file
        this_filestat = os.stat(filepath, follow_symlinks=False)
    except OSError as e:
        # e.g. FileNotFoundError, PermissionError
        #myerr.print("Filestat Error opening:\n"+filepath )
        #myerr.print("  Error: "+str(type(e)))
        #myerr.print("  Error: "+str(e))
        return (-1,-1,-1,[type(e),str(e)])
    except:
        e = sys.exc_info()
        myerr.print("UNHANDLED File Stat on: "+filepath)
        myerr.print("  Error: "+str(e[0]))
        myerr.print("  Error: "+str(e[1]))
        myerr.print("  Error: "+str(e[2]))
        return (-1,-1,-1,[str(e[0]),str(e[1]),str(e[2])])

    this_size = this_filestat.st_size
    this_mod = this_filestat.st_mtime
    this_blocks = this_filestat.st_blocks
    
    if IGNORE_FILES.get(os.path.basename(filepath),False):
        # TODO: experimental
        this_size = -1
        this_mod = -1
        this_blocks = this_blocks
        extra_info = ['ignore_files']
    elif os.path.islink(filepath):
        # skip symbolic links without commenting
        this_size = -1
        this_mod = -1
        this_blocks = this_blocks
        extra_info = ['symlink']
    elif stat.S_ISFIFO(this_filestat.st_mode):
        # skip FIFOs without commenting
        this_size = -1
        this_mod = -1
        this_blocks = -1
        extra_info = ['fifo']
    elif stat.S_ISSOCK(this_filestat.st_mode):
        # skip sockets without commenting
        this_size = -1
        this_mod = -1
        this_blocks = -1
        extra_info = ['socket']
    else:
        pass

    return (this_size, this_mod, this_blocks, extra_info)


# filetree is dict of dicts and items
#   each subdir is a nested dict subtree containing dicts and items
#   base of filetree corresponds to master_root
#   file item is [file_id, size]
# return valid pointer to subtree of tree corresponding to root dir
# base of dict tree is relative to string master_root
# root is string of dir to get dict of (also containing master_root string)
# create tree dict hierarchical structure if needed to get to root
def subtree_dict(filetree, root, master_root):
    # root includes master_root
    root_relative = os.path.relpath(root, start=master_root)
    #print( "  root_relative to master_root: " + root_relative)
    subtree = filetree
    for pathpart in root_relative.split( os.path.sep ):
        if pathpart and pathpart != '.':
            # either get pathpart key of subtree or create new one (empty dict)
            subtree = subtree.setdefault(pathpart,{})
    return subtree


# go through every file hierarchically inside argument paths
# make hash with keys being filesize in bytes and value being list of files
#   that match
def hash_files_by_size( paths, master_root ):
    unproc_files = []
    file_size_hash = {}
    filetree = {}
    fileblocks = {}
    filemodtimes = {}
    filesreport_time = time.time()
    filesdone = 0

    #.........................
    # local function to process one file
    def process_file_size():
        # read/write these from hash_files_by_size scope
        nonlocal filesdone, filesreport_time

        filepath = os.path.join(root,filename)
        (this_size,this_mod,this_blocks,extra_info) = check_stat_file(filepath)
        # if valid blocks then record for dir block tally
        if this_blocks != -1:
            fileblocks[filepath] = this_blocks
        if this_size == -1:
            unproc_files.append([filepath]+extra_info)
            return

        # set filename branch of filetree to -1 (placeholder, meaning no id)
        # adding to filetree means it will be taken into account when
        #   determining dir sameness
        # all ignored files that cause return above will be ignored for
        #   determining dir sameness
        subtree_dict(filetree, root, master_root)[filename] = -1

        # setdefault returns [] if this_size key is not found
        # append as item to file_size_hash [filepath,filemodtime] to check if
        #   modified later
        file_size_hash.setdefault(this_size,[]).append(filepath)
        filemodtimes[filepath] = this_mod

        filesdone+=1
        if filesdone%1000 == 0 or time.time()-filesreport_time > 15:
            myerr.print( "\r  "+str(filesdone)+" files sized.", end='', flush=True)
            filesreport_time = time.time()
    #.........................

    # Actual hierarchical file stat processing
    for treeroot in paths:
        myerr.print("Starting sizing of: "+treeroot)
        # remove trailing slashes, etc.
        treeroot = os.path.normpath(treeroot)
        if os.path.isdir( treeroot ):
            for (root,dirs,files) in os.walk(treeroot):
                # TODO: get modtime on directories too, to see if they change?
                for filename in files:
                    process_file_size()
        else:
            # this treeroot was a file
            (root,filename) = os.path.split(treeroot)
            process_file_size()

        # print final tally with CR
        myerr.print("\r  "+str(filesdone)+" files sized.")

    # tally unique, possibly duplicate files
    unique = 0
    nonunique = 0
    for key in file_size_hash.keys():
        if len(file_size_hash[key])==1:
            unique += 1
        else:
            nonunique += len(file_size_hash[key])
    myerr.print("\nUnique: %d    "%unique)
    myerr.print("Possibly Non-Unique: %d\n"%nonunique)

    return (file_size_hash, filetree, filemodtimes, fileblocks, unproc_files)


# datachunks_list: list of arrays
# match_groups: list of indicies_match_lists, indicies_match_list is all
#   indicies with identical arrays
def matching_array_groups(datachunks_list):
    match_groups = []
    # copy into remaining chunks
    ungrp_chunk_indicies = range(len(datachunks_list))
   
    # loop through chunks, looking for matches in unsearched chunks for first
    #   item in unsearched chunks
    #   item will always match itself, may match others
    #   save all matching indicies for this chunk into list of indicies
    #       appended to match_groups
    while(ungrp_chunk_indicies): # e.g. while len > 0
        #print(ungrp_chunk_indicies)
        test_idx = ungrp_chunk_indicies[0]
        matching_indicies = [i for i in ungrp_chunk_indicies if datachunks_list[i]==datachunks_list[test_idx]]
        match_groups.append(matching_indicies)
        ungrp_chunk_indicies = [x for x in ungrp_chunk_indicies if x not in matching_indicies]

    return match_groups


# all files in filelist are of size file_size (in blocks)
# dup_groups = [
#       [file_size1, [identical_file1a, identical_file1b, identical_file1c]],
#       [file_size2, [identical_file2a, identical_file2b]]
#               ]
def compare_file_group(filelist, fileblocks):
    #print('------ compare_file_group -----------')
    max_files_open = 1 # TODO: we can optimize by allowing multiple files open at the same time

    # amt_file_read starts small on first pass (most files will be caught)
    #   later it will be upped to max_file_read for next passes
    amt_file_read = 256

    # init empty lists to append to
    unique_files = []
    dup_groups = []
    unproc_files = []

    # check if this is too easy (only one file)
    if len(filelist)==1:
        #(unique_files,dup_groups,unproc_files)
        return (filelist,[],[])

    # right now only one prospective group of files, split later if distinct
    #   file groups are found
    filelist_groups_next=[filelist]

    # initial file position is 0
    filepos = 0

    # TODO: if total files is small enough  (< 100?), keep them all open while
    #   reading, close when they are called unique or dup  (Or at very end!)
    while(filelist_groups_next): # i.e. while len > 0
        filelist_groups = filelist_groups_next[:]
        # reset next groups
        filelist_groups_next = []

        # for debugging print current groups every time through
        #print([len(x) for x in filelist_groups])

        # each filelist_group is a possible set of duplicate files
        # a file is split off from a filelist_group as it is shown to be
        #   different from others in group, either to a subgroup of matching
        #   files or by itself
        for filelist_group in filelist_groups:
            filedata_list = []
            filedata_size_list = []
            # open files one at a time and close after getting each file's
            #   data into filedata_list
            for thisfile in filelist_group:
                try:
                    with open(thisfile,'rb') as thisfile_fh:
                        thisfile_fh.seek(filepos)
                        this_filedata = thisfile_fh.read(amt_file_read)
                    filedata_list.append(this_filedata)
                    # filedata_size_list is how many bytes we actually read
                    #   (may be less than max)
                    filedata_size_list.append(len(this_filedata))
                except OSError as e:
                    # e.g. FileNotFoundError, PermissionError
                    #myerr.print(str(e))
                    unproc_files.append([thisfile, str(type(e)), str(e) ])
                    # append -1 to signify invalid
                    filedata_list.append(-1)
                    filedata_size_list.append(-1)
                except:
                    e = sys.exc_info()
                    myerr.print("UNHANDLED Error opening:\n"+thisfile)
                    myerr.print("  Error: "+str(e[0]))
                    myerr.print("  Error: "+str(e[1]))
                    myerr.print("  Error: "+str(e[2]))
                    raise e[0]

            # remove invalid files from filelist_group, filedata_list,
            #   filedata_size_list
            invalid_idxs = [i for i in range(len(filedata_size_list)) if filedata_size_list[i]==-1]
            filelist_group = [x for (i,x) in enumerate(filelist_group) if i not in invalid_idxs]
            filedata_list= [x for (i,x) in enumerate(filedata_list) if i not in invalid_idxs]
            filedata_size_list= [x for (i,x) in enumerate(filedata_size_list) if i not in invalid_idxs]

            # get groups of indicies with datachunks that match each other
            match_idx_groups = matching_array_groups(filedata_list)

            single_idx_groups = [ x for x in match_idx_groups if len(x)==1 ]
            match_idx_groups = [ x for x in match_idx_groups if x not in single_idx_groups ]

            unique_files.extend([ filelist_group[sing_idx_group[0]] for sing_idx_group in single_idx_groups ])

            # we stop reading a file if it is confirmed unique, or if we get
            #   to the end of the file

            # for each group > 1 member, see if we need to keep searching it
            #   or got to end of files
            for match_idx_group in match_idx_groups:
                filedata_sizes_group = [filedata_size_list[i] for i in match_idx_group]
                # all filedata_sizes in matching group should be equal, so
                #   check the first one as representve
                if filedata_sizes_group[0] < amt_file_read:
                    # if entire matching group has less data than we tried to
                    #   read, we are at end of files and this is a final
                    #   dupgroup
                    this_dup_group_list = [filelist_group[i] for i in match_idx_group]
                    this_dup_blocks = fileblocks[this_dup_group_list[0]]
                    dup_groups.append([this_dup_blocks, this_dup_group_list])
                else:
                    # if filedata size is amt_file_read then not at end of
                    #   files, keep reading / checking
                    filelist_groups_next.append([filelist_group[i] for i in match_idx_group])

        # increment file position for reading next time through groups
        filepos = filepos + amt_file_read

        if filelist_groups_next: # i.e if non-empty
            # max file read is total memory to be used divided by num of files
            #   in largest group this iter
            # total no more than MEM_TO_USE
            max_file_read = MEM_TO_USE // max([len(x) for x in filelist_groups_next])
            #max_file_read = 5 # small for debugging
            if max_file_read < 5:
                raise Exception("compare_file_group: too many files to compare: "+str(len(filelist)))
        
            # after first pass dramatically increase file reads
            amt_file_read = max_file_read

    return (unique_files, dup_groups, unproc_files)


def compare_files(file_size_hash, fileblocks, unproc_files):
    unique_files = []
    dup_groups = []

    compare_files_timer = tictoc.Timer()
    compare_files_timer.start()
    myerr.print("Starting comparing file data")

    old_time = 0
    for key in file_size_hash.keys():
        (this_unique_files,this_dup_groups,this_unproc_files
                ) = compare_file_group(file_size_hash[key], fileblocks)
        unique_files.extend(this_unique_files)
        dup_groups.extend(this_dup_groups)
        unproc_files.extend(this_unproc_files)
        if compare_files_timer.eltime() > old_time+0.4:
            old_time = compare_files_timer.eltime()
            compare_files_timer.eltime_pr("\rElapsed: ",end='', file=sys.stderr)

    myerr.print("\nFinished comparing file data")

    return (dup_groups, unique_files)


# returns file_id dict with id item for every filepath key
def create_file_ids(dup_groups, unique_files, filetree, master_root):
    file_id = {}
    idnum = 0;
    for unq_file in unique_files:
        file_id[unq_file] = idnum

        # add id to tree
        (unq_dir,unq_file) = os.path.split(unq_file)
        subtree_dict(filetree, unq_dir, master_root)[unq_file] = idnum

        idnum += 1
    for dup_group in dup_groups:
        for dup_file in dup_group[1]:
            file_id[dup_file] = idnum

            # add id to tree
            (dup_dir,dup_file) = os.path.split(dup_file)
            subtree_dict(filetree, dup_dir, master_root)[dup_file] = idnum
        idnum += 1

    return file_id


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
                # if '.' : searchpath1 and searchpath2 are same path
                #   (search artifact)
                # if relpath doesn't start with .. , then searchpath1 is
                #   subdir of searchpath2
                remove_searchpath[searchpath1]=True
    for (i,searchpath) in enumerate(new_searchpaths):
        if remove_searchpath.get(searchpath,False):
            del(new_searchpaths[i])
        
    master_root = os.path.commonpath( new_searchpaths )
    return (master_root, new_searchpaths)


# name is filepath of this dir/file
# subtree is dict of this dir/file
def recurse_subtree(name, subtree, dir_dict, fileblocks):
    itemlist = []
    dir_blocks = 0
    for key in subtree.keys():
        # key is name of dir/file inside of this dir
        if type(subtree[key]) is dict:
            item = recurse_subtree(
                    os.path.join(name,key), subtree[key], dir_dict, fileblocks)
        else:
            item = str(subtree[key])
        dir_blocks += fileblocks[os.path.join(name,key)]
        itemlist.append(item)

    # put file blocks back into fileblocks db
    fileblocks[name] = dir_blocks

    # if any one item is "-1" (unknown file) then this whole directory is "-1"
    #   in this way we mark every subdir above unknown file as unknown
    if "-1" in itemlist:
        hier_id_str = "-1"
    else:
        itemlist.sort()
        hier_id_str = '['+','.join(itemlist)+']'

    dir_dict.setdefault(hier_id_str,[]).append(name)

    return hier_id_str


# inventory directories based on identical/non-identical contents (ignoring
#   file/dir names)
# dir_dict: key: hash based on dir hierarchical contents, item: dir path
def recurse_analyze_filetree(filetree, master_root, fileblocks, dup_groups ):
    dup_dirs = []
    unique_dirs = []
    dir_dict = {}

    # root_str is the string representation of the root of the filetree.  It
    #   shouldn't match anything else because it is highest
    # recurse_subtree creates a string representation of every subdir
    #   represented in filetree, based on the a hierarchical concatenation of
    #   the file ids in each subdir's hierarchy of files
    root_str = recurse_subtree(master_root, filetree, dir_dict, fileblocks)

    # unknown dirs show up with key of "-1"
    unknown_dirs = dir_dict.get("-1",[])
    if unknown_dirs:
        del(dir_dict["-1"])

    dup_dirs = [dir_dict[x] for x in dir_dict.keys() if len(dir_dict[x]) > 1]
    for i in range(len(dup_dirs)):
        # convert blocks to bytes to compare with dup_files
        dup_dirs[i] = [ fileblocks[dup_dirs[i][0]], [x+os.path.sep for x in dup_dirs[i]] ]

    unique_dirs = [dir_dict[x][0]+os.path.sep for x in dir_dict.keys() if len(dir_dict[x]) == 1]

    dup_groups.extend(dup_dirs)
    return (unique_dirs,unknown_dirs)


def print_sorted_dups(dup_groups, master_root):
    print("")
    print("Duplicate Files/Directories:")
    for dup_group in sorted(dup_groups, reverse=True, key=lambda x: x[0]):
        print("Duplicate set (%sB each)"%( size2eng(512*dup_group[0]) ))
        for filedir in sorted(dup_group[1]):
            if master_root == "/":
                # all paths are abspaths
                filedir_str = filedir
            else:
                # relpath from master_root
                filedir_str = os.path.relpath(filedir, start=master_root)
                if filedir.endswith(os.path.sep):
                    filedir_str += os.path.sep
            print("  %s"%filedir_str)


def print_sorted_uniques(unique_files, master_root):
    print("\n")
    print("Unique Files/Directories:")
    for filedir in sorted(unique_files):
        if master_root == "/":
            # all paths are abspaths
            filedir_str = filedir
        else:
            # relpath from master_root
            filedir_str = os.path.relpath(filedir, start=master_root)
            if filedir.endswith(os.path.sep):
                filedir_str += os.path.sep
        print(filedir_str)


def print_unproc_files(unproc_files):
    symlinks = [x[0] for x in unproc_files if x[1]=="symlink"]
    ignored = [x[0] for x in unproc_files if x[1]=="ignore_files"]
    sockets = [x[0] for x in unproc_files if x[1]=="socket"]
    fifos = [x[0] for x in unproc_files if x[1]=="fifo"]

    other = [x for x in unproc_files if x[0] not in symlinks]
    other = [x for x in other if x[0] not in ignored]
    other = [x for x in other if x[0] not in sockets]
    other = [x for x in other if x[0] not in fifos]

    print("\n")
    print("Unprocessed Files")
    if other:
        print("\nUnreadable Files (ignored)")
        for err_file in sorted(other):
            print("  "+err_file[0])
            for msg in err_file[1:]:
                err_str = textwrap.fill(
                        msg, initial_indent=' '*2, subsequent_indent=' '*6)
                print(err_str)
    if sockets:
        print("\nSockets (ignored)")
        for sock_file in sorted(sockets):
            print("  "+sock_file)
    if fifos:
        print("\nFIFOs (ignored)")
        for fifo_file in sorted(fifos):
            print("  "+fifo_file)
    if symlinks:
        print("\nSymbolic Links (ignored)")
        for symlink in sorted(symlinks):
            print("  "+symlink)
    if ignored:
        print("\nIgnored Files")
        for ignore_file in sorted(ignored):
            print("  "+ignore_file)


def print_header(master_root):
    if master_root != "/":
        print("All file paths referenced from:\n"+master_root)


def print_unknown_dirs(unknown_dirs)
    if unknown_dirs:
        print("\nUnknown Dirs")
        for unk_dir in sorted(unknown_dirs):
            print(unk_dir)


#   1. For every file, get: size, mod_time
#   2. hash by sizes, each hash size in dict fill with list of all files
#       that size
#   3. go through each hash, deciding which of the list are unique or same
#       by comparing matching-size files chunk by chunk, splitting into
#       subgroups as differences found
def main(argv=None):
    mytimer = tictoc.Timer()
    mytimer2 = tictoc.Timer()
    mytimer2.start()
    args = process_command_line(argv)

    # eliminate duplicates, and paths that are sub-paths of other searchpaths
    (master_root, searchpaths) = process_searchpaths(args.searchpaths)
    
    # file_size_hash is dict: keys are file sizes in bytes, items are lists of
    #   filepaths that are all that size (lists are all len > 1)
    # filemodtimes is dict: keys are filepaths, items are modtimes when file
    #   was stat'ed
    (file_size_hash, filetree, filemodtimes, fileblocks, unproc_files
            ) = hash_files_by_size(searchpaths, master_root)

    # compare all filegroups by using byte-by-byte comparison to find actually
    #   unique, duplicate
    # dupgroups: list of lists of identical files
    # uniquefiles: list unique files
    # unproc_files: list problematic (mostly unreadable) files
    (dup_groups, unique_files) = compare_files(
            file_size_hash, fileblocks, unproc_files)

    # now we know all of the files that are duplicates and unique
    # we will also determine below which directories are identical in that
    #   they contain identical files and subdirs

    # for each unique file, make a unique id, identical-data files share id
    #   save unique id to file_id dict (key=filepath, item=id)
    #   save unique id to items of files in filetree hierarchical dict
    file_id = create_file_ids(dup_groups, unique_files, filetree, master_root)

    # inventory directories based on identical/non-identical contents
    #   (ignoring names)
    # unknown_dirs are any dir that we couldn't check any of the files due to
    #   file issues, etc.
    # file_size_hash gives info on file sizes, used to compute directory total
    #   size
    (unique_dirs, unknown_dirs) = recurse_analyze_filetree(
            filetree, master_root, fileblocks, dup_groups)

    # PRINT REPORT

    # header for report
    print_header(master_root)

    # print a sorted (biggest dir/files first) list of dup groups,
    #   alphabetical within each group
    print_sorted_dups(dup_groups, master_root)

    # print a sorted (alphabetical) list of unique files and dirs
    unique_files.extend(unique_dirs)
    print_sorted_uniques(unique_files, master_root)

    # print lists of unprocessed files
    print_unproc_files(unproc_files)

    # print unknown status directories
    print_unknown_dirs(unknown_dirs)

    print("")
    mytimer2.eltime_pr("Total Elapsed time: ", file=sys.stderr )
    mytimer2.eltime_pr("Total Elapsed time: ", file=sys.stdout )
    return 0

if __name__ == '__main__':
    status = main(sys.argv)
    sys.exit(status)
