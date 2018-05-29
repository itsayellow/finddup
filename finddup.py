#!/usr/bin/env python3

"""Find duplicate files, dirs based on their data, not names.
    Finds identical files, dirs even if they have different names.
    Searches hierarchically through all paths.
    Doesn't follow symbolic links
    Ignores: symbolic links, fifos, sockets, certain system info files
        like: .picasa.ini, .DS_Store, Thumbs.db, " Icon\r"
"""

# TODO: We need to have two classes of problem files: 1.) ignored, don't
#   matter for dir compare and 2.) read error, cause dir compare to be
#   unknown
# TODO: asterisk dirs that are dups if they contain ignored files
# TODO: nice to know if a directory contains only matching files, even if that
#   directory doesn't match another directory completely
#   i.e. dir1 is subset of dir2
#     e.g. DIR1: fileA, fileB
#          DIR2: fileA, fileB, fileC
#     still might want to delete DIR1 even though it doesn't match exactly DIR2
# TODO: could check if duplicate files have same inode? (hard link)?
#   maybe too esoteric

import os
import os.path
import stat
import sys
import argparse
import time
import textwrap
#import subprocess
#import re
#from functools import partial
#import multiprocessing.pool
import tictoc


# how much total memory bytes to use during comparison of files
#   (Larger is faster up to a point)
MEM_TO_USE = 512*1024*1024    # 512MB
MEM_TO_USE = 2*1024*1024*1024 # 2GB
MEM_TO_USE = 1024*1024*1024   # 1GB

# how many files we can have open at the same time
MAX_FILES_OPEN = 200

# TODO more generalized way of specifying this
IGNORE_FILES = {
        ".picasa.ini":True,
        ".DS_Store":True,
        "Thumbs.db":True,
        " Icon\r":True,
        "Icon\r":True
        }


class StderrPrinter(object):
    """Prints to stderr especially for use with \r and same-line updates

    Keeps track of whether an extra \n is needed before printing string,
    especially in cases where the previous print string didn't have
    one and this print string doesn't start with \r

    Allows for easily printing error messages (regular print) amongst
    same-line updates (starting with \r and with no finishing \n).
    """
    def __init__(self):
        self.need_cr = False

    def print(self, text, **prkwargs):
        """Print to stderr, automatically knowing if we need a CR beforehand.
        """
        if text.startswith('\r'):
            self.need_cr = False
        # we need_cr if last print specifically didn't have a \n,
        #   and this one doesn't start with \r
        # Most likely last one was a progress display and this one is an
        #   error or warning.
        # Instead of printing on the end of the line after the progress
        #   line, it \n to the next line.
        # [It could just as easily be a \r to erase the previous line.]
        if self.need_cr:
            print("", file=sys.stderr)

        print(text, file=sys.stderr, **prkwargs)

        if prkwargs.get('end', '\n') == '' and not text.endswith('\n'):
            self.need_cr = True
        else:
            self.need_cr = False


# Global
myerr = StderrPrinter()


def process_command_line(argv):
    """Process command line invocation arguments and switches.

    Args:
        argv: list of arguments, or `None` from ``sys.argv[1:]``.

    Returns:
        args: Namespace with named attributes of arguments and switches
    """
    argv = argv[1:]

    # initialize the parser object:
    parser = argparse.ArgumentParser(
            description="Find duplicate files and directories in all paths.  "\
                    "Looks at file content, not names or info.")

    # specifying nargs= puts outputs of parser in list (even if nargs=1)

    # required arguments
    parser.add_argument('searchpaths', nargs='+', metavar='searchpath',
            help="Search path(s) (recursively searched)."
            )

    # switches/options:
    parser.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='Verbose status messages.')

    #(settings, args) = parser.parse_args(argv)
    args = parser.parse_args(argv)

    return args


def num2eng(num, k=1024):
    """Convert input num to string with unit prefix

    Copied from durank.
    Use k=1024 for binary (e.g. kB).
    Use k=1000 for non-binary kW.

    Args:
        num: integer amount
        k: the amount that is 1k, usually 1000 or 1024.  Default is 1024

    Returns:
        numstr: string of formatted decimal number with unit prefix at end
    """
    if   num > k**5:
        numstr = "%.1fP" % (float(num)/k**5)
    elif num > k**4:
        numstr = "%.1fT" % (float(num)/k**4)
    elif num > k**3:
        numstr = "%.1fG" % (float(num)/k**3)
    elif num > k**2:
        numstr = "%.1fM" % (float(num)/k**2)
    elif num > k:
        numstr = "%.1fk" % (float(num)/k)
    else:
        numstr = "%.1g" % (float(num))
    return numstr


def check_stat_file(filepath):
    """Get file's stat from os, and handle files we ignore.

    Get filestat on file if possible (i.e. readable), discard if symlink,
    pipe, fifo, or one of set of ignored files.  Return this_size is -1
    if discarded file.  All files possible to stat return valid blocks
    (to allow for parent dir sizing later).

    Args:
        filepath: path to file to check

    Returns:
        this_size: integer size of file in bytes from file stat.  -1 if
            skipped
        this_mod: modification time of file from file stat
        this_blocks: integer size of file in blocks from file stat
        extra_info: list, usually information explaining a skipped or
            errored un-stat'ed file
    """
    extra_info = []

    try:
        # don't follow symlinks, just treat them like a regular file
        this_filestat = os.stat(filepath, follow_symlinks=False)
    except OSError as e:
        # e.g. FileNotFoundError, PermissionError
        #myerr.print("Filestat Error opening:\n"+filepath )
        #myerr.print("  Error: "+str(type(e)))
        #myerr.print("  Error: "+str(e))
        return (-1, -1, -1, [type(e), str(e)])
    except KeyboardInterrupt:
        # get out if we get a keyboard interrupt
        raise
    except:
        # this is really an internal error and should never happen
        e = sys.exc_info()
        myerr.print("UNHANDLED File Stat on: "+filepath)
        myerr.print("  Error: "+str(e[0]))
        myerr.print("  Error: "+str(e[1]))
        myerr.print("  Error: "+str(e[2]))
        return (-1, -1, -1, [str(e[0]), str(e[1]), str(e[2])])

    this_size = this_filestat.st_size
    this_mod = this_filestat.st_mtime
    try:
        this_blocks = this_filestat.st_blocks
    except AttributeError:
        # Windows has no st_blocks attribute
        this_blocks = this_size//512 + (1 if this_size%512 != 0 else 0)

    if IGNORE_FILES.get(os.path.basename(filepath), False):
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


def subtree_dict(filetree, root, master_root):
    """Return a subtree dict part of filetree master hierarchical dict

    filetree is dict of dicts and items.  Each subdir is a nested dict
    subtree containing dicts and items.  The base of filetree corresponds
    to path master_root.  Keys are file/dir names.  Each file item is size
    in blocks.  Items with -1 signify unknown block-size.  Each dir item
    is another dict holding the dirs/files in that dir as keys.

    The base of filetree corresponds to path master_root.  This creates tree
    dict hierarchical structure if needed to get to root.

    Args:
        filetree: dict of dicts and items, representing full file tree of
            all searched paths
        root: filepath of desired dict subtree (absolute path preferred).
        master_root: string that is highest common root dir for all
            searched files, dirs.  Corresponds to root of dict filetree

    Returns:
        subtree: dict of filetree for root dir
    """
    # root includes master_root
    root_relative = os.path.relpath(root, start=master_root)
    #print( "  root_relative to master_root: " + root_relative)
    subtree = filetree
    for pathpart in root_relative.split(os.path.sep):
        if pathpart and pathpart != '.':
            # either get pathpart key of subtree or create new one (empty dict)
            subtree = subtree.setdefault(pathpart, {})
    return subtree


def matching_array_groups(datachunks_list):
    """Return identical indicies groups from list of data chunks.

    Args:
        datachunks_list: list of arrays of data, all same size

    Returns:
        match_idx_groups: list of indicies_match_lists for matching data
            arrays, each sublist has greater than one member
        single_idx_groups: list of indicies for data arrays that don't
            match any other data array (singletons)
    """
    match_idx_groups = []
    # copy into remaining chunks
    ungrp_chunk_indicies = range(len(datachunks_list))

    # loop through chunks, looking for matches in unsearched chunks for first
    #   item in unsearched chunks
    #   item will always match itself, may match others
    #   save all matching indicies for this chunk into list of indicies
    #       appended to match_idx_groups
    while ungrp_chunk_indicies: # e.g. while len > 0
        test_idx = ungrp_chunk_indicies[0]

        matching_indicies = []
        for i in ungrp_chunk_indicies:
            if datachunks_list[i] == datachunks_list[test_idx]:
                matching_indicies.append(i)

        match_idx_groups.append(matching_indicies)
        ungrp_chunk_indicies = [
                x for x in ungrp_chunk_indicies if x not in matching_indicies]

    single_idx_groups = [x[0] for x in match_idx_groups if len(x) == 1]
    match_idx_groups = [x for x in match_idx_groups if x[0] not in single_idx_groups]

    return (match_idx_groups, single_idx_groups)


def read_filehandle_list(fhlist_group, amt_file_read):
    """Read amt_file_read bytes in each of list of open filehandles.

    It is assumed that all files in fhlist_group are the same size in
    bytes.

    Args:
        fhlist_group: list of open file handles to read
        amt_file_read: amount of bytes to read from each file

    Returns:
        fhlist_group_new: version of fhlist_group with unproc_files
            removed
        filedata_list: list of arrays of read file data
        unproc_files: files that were unreadable due to errors
        file_bytes_read: actual number of bytes read from every valid file
    """
    filedata_list = []
    filedata_size_list = []
    unproc_files = []
    # open files one at a time and close after getting each file's
    #   data into filedata_list
    for thisfh in fhlist_group:
        try:
            this_filedata = thisfh.read(amt_file_read)
            filedata_list.append(this_filedata)
            # filedata_size_list is how many bytes we actually read
            #   (may be less than max)
            filedata_size_list.append(len(this_filedata))
        except OSError as e:
            # e.g. FileNotFoundError, PermissionError
            #myerr.print(str(e))
            unproc_files.append([thisfh.name, str(type(e)), str(e)])
            # append -1 to signify invalid
            filedata_list.append(-1)
            filedata_size_list.append(-1)
        except KeyboardInterrupt:
            # get out if we get a keyboard interrupt
            raise
        except:
            # this is really an internal error and should never happen
            e = sys.exc_info()
            myerr.print("UNHANDLED Error opening:\n"+thisfh.name)
            myerr.print("  Error: "+str(e[0]))
            myerr.print("  Error: "+str(e[1]))
            myerr.print("  Error: "+str(e[2]))
            raise e[0]

    # remove invalid files from fhlist_group, filedata_list,
    #   filedata_size_list
    invalid_idxs = [i for i in range(len(filedata_size_list)) if filedata_size_list[i] == -1]
    fhlist_group_new = [x for (i, x) in enumerate(fhlist_group) if i not in invalid_idxs]
    filedata_list = [x for (i, x) in enumerate(filedata_list) if i not in invalid_idxs]
    filedata_size_list = [x for (i, x) in enumerate(filedata_size_list) if i not in invalid_idxs]

    if filedata_size_list:
        file_bytes_read = filedata_size_list[0]
    else:
        # all are invalid
        file_bytes_read = 0
    return (filedata_list, fhlist_group_new, unproc_files, file_bytes_read)


def read_filelist(filelist_group, filepos, amt_file_read):
    """Read amt_file_read bytes starting at filepos in list of files.

    It is assumed that all files in filelist_group are the same size in
    bytes.

    Args:
        filelist_group: list of files to read
        filepos: starting byte position when reading each file
        amt_file_read: amount of bytes to read from each file

    Returns:
        filelist_group_new: version of filelist_group with unproc_files
            removed
        filedata_list: list of arrays of read file data
        unproc_files: list of files that were unreadable due to errors,
            each item in list:
            [filename, error_type, error_description]
        file_bytes_read: actual number of bytes read from every valid file
    """
    filedata_list = []
    filedata_size_list = []
    unproc_files = []
    # open files one at a time and close after getting each file's
    #   data into filedata_list
    for thisfile in filelist_group:
        try:
            with open(thisfile, 'rb') as thisfile_fh:
                thisfile_fh.seek(filepos)
                this_filedata = thisfile_fh.read(amt_file_read)
            filedata_list.append(this_filedata)
            # filedata_size_list is how many bytes we actually read
            #   (may be less than max)
            filedata_size_list.append(len(this_filedata))
        except OSError as e:
            # e.g. FileNotFoundError, PermissionError
            #myerr.print(str(e))
            unproc_files.append([thisfile, str(type(e)), str(e)])
            # append -1 to signify invalid
            filedata_list.append(-1)
            filedata_size_list.append(-1)
        except KeyboardInterrupt:
            # get out if we get a keyboard interrupt
            raise
        except:
            # this is really an internal error and should never happen
            e = sys.exc_info()
            myerr.print("UNHANDLED Error opening:\n"+thisfile)
            myerr.print("  Error: "+str(e[0]))
            myerr.print("  Error: "+str(e[1]))
            myerr.print("  Error: "+str(e[2]))
            raise e[0]

    # remove invalid files from filelist_group, filedata_list,
    #   filedata_size_list
    invalid_idxs = [i for i in range(len(filedata_size_list)) if filedata_size_list[i] == -1]
    filelist_group_new = [x for (i, x) in enumerate(filelist_group) if i not in invalid_idxs]
    filedata_list = [x for (i, x) in enumerate(filedata_list) if i not in invalid_idxs]
    filedata_size_list = [x for (i, x) in enumerate(filedata_size_list) if i not in invalid_idxs]

    if filedata_size_list:
        file_bytes_read = filedata_size_list[0]
    else:
        # all are invalid
        file_bytes_read = 0
    return (filedata_list, filelist_group_new, unproc_files, file_bytes_read)


def compare_file_group(filelist, fileblocks):
    """Compare data in files, find groups of identical and unique files.

    It is assumed that all files in filelist are the same size in bytes.

    Go through data in files in each group, finding files in each group
    that are identical or unique with respect to their data.

    This could have been done using recursion, but for big files might
    have taken so many file chunks that it would've taken too many
    levels of recursion and overflowed the stack.

    Args:
        filelist: list of lists of files where each sublist is a list
            of files with the same size as each other.  Each sublist:
            [<size in blocks of all files>, [file1, file2, ...]
        fileblocks: dict with key: filepath, item: file size in blocks

    Returns:
        unique_files: list of files that are verified unique
        dup_groups: list of lists of files that are verified duplicate
            data files.  Each sublist:
            [<size in blocks of all files>, [file1, file2, ...]
        unproc_files: list of files that cannot be opened or read
    """
    # init empty lists to append to
    unique_files = []
    dup_groups = []
    unproc_files = []

    # check if this is too easy (only one file)
    if len(filelist) == 1:
        #(unique_files,dup_groups,unproc_files)
        return (filelist, [], [])

    # initial file position is 0
    filepos = 0

    # amt_file_read starts small on first pass (most files will be caught)
    #   later it will be upped to maximum for next passes
    amt_file_read = 256

    # right now only one prospective group of files, split later if distinct
    #   file groups are found
    # If group is small enough, we can keep all files open while reading
    # If group is too big, we open each file one at a time
    open_filehandles = []
    if len(filelist) < MAX_FILES_OPEN:
        try:
            for filename in filelist:
                try:
                    fh = open(filename, 'rb')
                except OSError as e:
                    # e.g. FileNotFoundError, PermissionError
                    #myerr.print(str(e))
                    unproc_files.append([filename, str(type(e)), str(e)])
                    #print("Can't open "+filename)
                except KeyboardInterrupt:
                    # get out if we get a keyboard interrupt
                    raise
                except:
                    # this is really an internal error and should never happen
                    print("UNHANDLED ERROR WITH OPENING "+filename)
                    unproc_files.append(filename)
                    print(unproc_files)
                else:
                    open_filehandles.append(fh)
            # list containing one item that is a list of filehandles
            filelist_groups_next = [open_filehandles[:]]
            # TODO: what if open_filehandles is empty or 1 file due
            #   to file open errors?  how does rest of code handle that?
        except KeyboardInterrupt:
            # if we get a keyboard interrupt, close all open handles
            #   and get out
            for fh in open_filehandles:
                fh.close()
            raise
    else:
        # list containing one item that is a list of filenames
        filelist_groups_next = [filelist]
    try:
        while filelist_groups_next: # i.e. while len > 0
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
                if open_filehandles:
                    # in this case, filelist_group is a list of filehandles
                    (filedata_list,
                            filelist_group,
                            this_unproc_files,
                            file_bytes_read
                            ) = read_filehandle_list(filelist_group,
                                    amt_file_read)
                    unproc_files.extend(this_unproc_files)
                else:
                    # in this case, filelist_group is a list of strings
                    #   specifying filenames
                    (filedata_list,
                            filelist_group,
                            this_unproc_files,
                            file_bytes_read
                            ) = read_filelist(filelist_group,
                                    filepos,
                                    amt_file_read)
                    unproc_files.extend(this_unproc_files)

                # get groups of indicies with datachunks that match each other
                (match_idx_groups, single_idx_groups) = matching_array_groups(
                        filedata_list)

                # add to list of unique files for singleton groups
                if open_filehandles:
                    unique_files.extend(
                            [filelist_group[s_i_g].name for s_i_g in single_idx_groups])
                else:
                    unique_files.extend(
                            [filelist_group[s_i_g] for s_i_g in single_idx_groups])

                # we stop reading a file if it is confirmed unique, or if we get
                #   to the end of the file

                # for each group > 1 member, see if we need to keep searching it
                #   or got to end of files
                for match_idx_group in match_idx_groups:
                    if file_bytes_read < amt_file_read:
                        # if bytes read is less data than we tried to
                        #   read, we are at end of files and this is a final
                        #   dupgroup
                        if open_filehandles:
                            this_dup_group_list = [filelist_group[i].name for i in match_idx_group]
                        else:
                            this_dup_group_list = [filelist_group[i] for i in match_idx_group]
                        this_dup_blocks = fileblocks[this_dup_group_list[0]]
                        dup_groups.append([this_dup_blocks, this_dup_group_list])
                    else:
                        # if filedata size is amt_file_read then not at end of
                        #   files, keep reading / checking
                        filelist_groups_next.append(
                                [filelist_group[i] for i in match_idx_group])

            # increment file position for reading next time through groups
            filepos = filepos + amt_file_read

            if filelist_groups_next: # i.e if non-empty
                # after first pass dramatically increase file read size to max
                # max file read is total memory to be used divided by num of files
                #   in largest group this iter
                # total no more than MEM_TO_USE
                amt_file_read = MEM_TO_USE // max([len(x) for x in filelist_groups_next])
                #amt_file_read = 5 # small for debugging
                if amt_file_read < 5:
                    raise Exception(
                            "compare_file_group: too many files to compare: " \
                                    + str(len(filelist)))
    finally:
        # whatever happens, make sure we close all open filehandles in this
        #   group
        for fh in open_filehandles:
            #print("Closing " + fh.name)
            fh.close()


    return (unique_files, dup_groups, unproc_files)


def create_file_ids(dup_groups, unique_files, filetree, master_root):
    """Create ID numbers for every file based on file data uniqueness

    This function adds file id numbers to each file in the filetree
    structure.  Files with unique data have unique file IDs.
    File IDs for two files are the same if the files' data are the same.

    Args:
        dup_groups: list of lists of filepaths that have duplicate data.
            Each list contains:
            [size in blocks of duplicate files, list of duplicate files]
        unique_files: list of paths of files that have unique data
        filetree: READ/WRITE dict of dicts and items representing
            hierarchy of all files searched starting at master_root,
            keys are file or dir name at that level, items are dict
            (if dir) or integer file id (if file).  File id is unique
            if file is unique (based on data).  Files with identical
            data inside have same file id
        master_root: string that is lowest common root dir for all
            searched files, dirs
    """
    file_id = {}
    idnum = 0
    for unq_file in unique_files:
        file_id[unq_file] = idnum

        # add id to tree
        (unq_dir, unq_file) = os.path.split(unq_file)
        subtree_dict(filetree, unq_dir, master_root)[unq_file] = idnum

        idnum += 1
    for dup_group in dup_groups:
        for dup_file in dup_group[1]:
            file_id[dup_file] = idnum

            # add id to tree
            (dup_dir, dup_file) = os.path.split(dup_file)
            subtree_dict(filetree, dup_dir, master_root)[dup_file] = idnum
        idnum += 1


def recurse_subtree(name, subtree, dir_dict, fileblocks):
    """Recurse subtree of filetree, at each dir saving dir data id, size.

    Directories are handled after files, because the ID string for a dir
    is based on the dir/file IDs hierarchically contained in that dir.

    Recursion causes lowest leaf dirs to be ID'ed first

    Every dir ID string is alphabetized to ensure the same order for the
    same set of file IDs.

    Saves dir IDs into dir_dict.  Saves dir size in blocks into fileblocks.
    Example:
        dir A contains: dir B, file C (ID: 345)
        dir B contains: file D (ID: 401), file E (ID: 405)
        ID string for dir B: [401,405]
        ID string for dir A: [[401,405],345]

    Args:
        name: name of filepath of this directory
        subtree: dict in filetree of this directory
        dir_dict: READ/WRITE key: hier_id_str, item: list of dir paths
            with this ID string
        fileblocks: READ/WRITE dict with key: filepath, item: size in blocks

    Returns:
        hier_id_str: string based only on fileids of files/dirs inside
            dir, specifying all fileids of files/dirs inside this dir
            hierarchically down to lowest levels
    """
    itemlist = []
    dir_blocks = 0
    for key in subtree.keys():
        # key is name of dir/file inside of this dir
        if isinstance(subtree[key], dict):
            item = recurse_subtree(
                    os.path.join(name, key), subtree[key], dir_dict, fileblocks)
        else:
            item = str(subtree[key])
        dir_blocks += fileblocks[os.path.join(name, key)]
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

    dir_dict.setdefault(hier_id_str, []).append(name)

    return hier_id_str


def recurse_analyze_filetree(filetree, master_root, fileblocks, dup_groups):
    """Create dir ids for each dir represented in filetree and find dups, etc.

    Inventory directories based on identical/non-identical data in files
    in the hierarchy of each directory (ignoring file/dir names)

    Create unique ID string for each directory that has unique hierarchical
    contents (based on file data).  For directories that have identical
    hierarchical files/data, give the same ID string.

    Find duplicate directories, and save their total size in blocks.  Also
    find unique directories.

    dir_dict: key: hash based on dir hierarchical contents, item: dir path

    Args:
        filetree: READ/WRITE
        master_root: string that is lowest common parent dir path of all
            searched files
        fileblocks: READ/WRITE dict where key is path to file/dir, item
            is size of file/dir in blocks
        dup_groups: READ/WRITE list of duplicate file/dir groups.  Duplicate
            directory groups are added to this.  Format for each sublist
            of this list:
            [size in blocks of duplicate dirs, list of duplicate dirs]

    Returns:
        unique_dirs: list of directories that are not a duplicate of
            any other known directory
        unknown_dirs: list of directories with an unknown file in the
            hierarchy, making these directories also "unknown"
    """
    dup_dirs = []
    unique_dirs = []
    dir_dict = {}

    # recurse_subtree creates a string representation of every subdir
    #   represented in filetree, based on the a hierarchical concatenation of
    #   the file ids in each subdir's hierarchy of files
    recurse_subtree(master_root, filetree, dir_dict, fileblocks)

    # unknown dirs show up with key of "-1", don't consider them for matching
    unknown_dirs = dir_dict.get("-1", [])
    # add trailing slash to all dir names
    unknown_dirs = [x + os.path.sep for x in unknown_dirs]
    if unknown_dirs:
        del dir_dict["-1"]

    # find set of unique dirs, sets of duplicate dirs
    for dirkey in dir_dict:
        # first dir path in group (only one if group size = 1)
        first_dir = dir_dict[dirkey][0]
        if len(dir_dict[dirkey]) > 1:
            # duplicate dir group
            this_blocks = fileblocks[first_dir]
            dup_dirs.append(
                    [this_blocks, [x+os.path.sep for x in dir_dict[dirkey]]])
        elif len(dir_dict[dirkey]) == 1:
            # unique dirs
            unique_dirs.append(first_dir + os.path.sep)
        else:
            raise Exception("Internal error: recurse_analyze_filetree has"\
                    " zero-size dir group.")
    dup_groups.extend(dup_dirs)

    return (unique_dirs, unknown_dirs)


def filedir_rel_master_root(filedir, master_root):
    """ Returns the path of filedir relative to master_root.

    If master_root is / (filesystem root dir) then return full absolute path.

    Args:
        filedir: path to be translated to relpath of master_root
        master_root: string of root of all searched paths

    Returns:
        filedir_str: filedir path relative to master_root, or absolute if
            master_root == "/"
    """
    if master_root == "/":
        # all paths are abspaths
        filedir_str = filedir
    else:
        # relpath from master_root
        filedir_str = os.path.relpath(filedir, start=master_root)
        if filedir.endswith(os.path.sep):
            filedir_str += os.path.sep
    return filedir_str


def print_sorted_dups(dup_groups, master_root):
    """Print report of sorted duplicate files and directories.

    Sort duplicate groups based on total size in blocks, biggest size first.

    Args:
        dup_groups: list of duplicate file/dir groups.  Format for each
            sublist of this list:
            [size in blocks of duplicate dirs, list of duplicate dir paths]
        master_root: string that is lowest common parent dir path of all
            searched files
    """
    print("")
    print("Duplicate Files/Directories:")
    print("----------------")
    for dup_group in sorted(dup_groups, reverse=True, key=lambda x: x[0]):
        print("Duplicate set (%sB each)"%(num2eng(512*dup_group[0])))
        for filedir in sorted(dup_group[1]):
            filedir_str = filedir_rel_master_root(filedir, master_root)
            print("  %s"%filedir_str)


def print_sorted_uniques(unique_files, master_root):
    """Print report of sorted list of unique files and directories

    Sort list of unique files and directories alphabetically.

    Args:
        unique_files: list of unique file/dir paths
        master_root: string that is lowest common parent dir path of all
            searched files
    """
    print("\n\nUnique Files/Directories:")
    print("----------------")
    for filedir in sorted(unique_files):
        filedir_str = filedir_rel_master_root(filedir, master_root)
        print(filedir_str)


def print_unproc_files(unproc_files, master_root):
    """Print report of all files unable to be processed.

    Any files that are unreadable are listed alphabetically.

    Args:
        unproc_files: list of unprocessed file paths
    """
    symlinks = [x[0] for x in unproc_files if x[1] == "symlink"]
    ignored = [x[0] for x in unproc_files if x[1] == "ignore_files"]
    sockets = [x[0] for x in unproc_files if x[1] == "socket"]
    fifos = [x[0] for x in unproc_files if x[1] == "fifo"]
    changes = [x[0] for x in unproc_files if x[1] == "changed"]

    # other is anything not in one of the above lists
    other = [x for x in unproc_files if x[0] not in symlinks]
    other = [x for x in other if x[0] not in ignored]
    other = [x for x in other if x[0] not in sockets]
    other = [x for x in other if x[0] not in fifos]
    other = [x for x in other if x[0] not in changes]

    print("\n\nUnprocessed Files")
    if other:
        print("\n\nUnreadable Files (ignored)")
        print("----------------")
        for err_file in sorted(other):
            filedir_str = filedir_rel_master_root(err_file[0], master_root)
            print("  "+filedir_str)
            for msg in err_file[1:]:
                err_str = textwrap.fill(
                        msg, initial_indent=' '*2, subsequent_indent=' '*6)
                print(err_str)
    if sockets:
        print("\n\nSockets (ignored)")
        print("----------------")
        for filedir in sorted(sockets):
            filedir_str = filedir_rel_master_root(filedir, master_root)
            print("  "+filedir_str)
    if fifos:
        print("\n\nFIFOs (ignored)")
        print("----------------")
        for filedir in sorted(fifos):
            filedir_str = filedir_rel_master_root(filedir, master_root)
            print("  "+filedir_str)
    if symlinks:
        print("\n\nSymbolic Links (ignored)")
        print("----------------")
        for filedir in sorted(symlinks):
            filedir_str = filedir_rel_master_root(filedir, master_root)
            print("  "+filedir_str)
    if changes:
        print("\n\nChanged Files (since start of this program's execution)")
        print("----------------")
        for filedir in changes:
            filedir_str = filedir_rel_master_root(filedir, master_root)
            print("  "+filedir_str)
    if ignored:
        print("\n\nIgnored Files")
        print("----------------")
        for filedir in sorted(ignored):
            filedir_str = filedir_rel_master_root(filedir, master_root)
            print("  "+filedir_str)


def print_unknown_dirs(unknown_dirs, master_root):
    """Print report of all files unable to be processed.

    Any directories that contain unreadable files listed alphabetically.

    Args:
        unknown_dirs: list of directory paths for dirs that have
            unreadable files
    """
    if unknown_dirs:
        print("\n\nUnknown Dirs")
        print("----------------")
        for filedir in sorted(unknown_dirs):
            filedir_str = filedir_rel_master_root(filedir, master_root)
            print("  "+filedir_str)


def print_header(master_root):
    """Print header information to start report on files and directories.

    Args:
        master_root: string that is lowest common parent dir path of all
            searched files
    """
    if master_root != "/":
        print("All file paths referenced from:\n"+master_root)


def get_frequencies(file_size_hash):
    """Collect data on equi-size file groups.

    This is not for user operation of this program.  This is an optional
    data collection for the programmer.

    Results:
    For each group of files that are of the same size in bytes:
        Groups of less than 46 files each account for 99% of groups
        Groups of less than 165 files each account for 99.9% of groups
        Groups of less than 250 files each account for 99.93% of groups

    Key takeaway: for the vast majority of groups, we should be able to
    keep all files in a group open at the same time and not exceed OS
    limits on open filehandles.

    Poisson(gamma=0.5) Distribution? (not exactly but close...)

    Args:
        file_size_hash: dict with key: filesize, item: list of files that are
            that size.  (e.g. returned from hash_files_by_size() )

    Returns:
        freq_dict: key: len of file list in file_size_hash,
            item: how many file groups in file_size_hash are that size
    """
    freq_dict = {}
    for key in file_size_hash:
        numfiles = len(file_size_hash[key])
        freq_dict[numfiles] = freq_dict.get(numfiles, 0) + 1

    for key in sorted(freq_dict):
        print("%d: %d hits"%(key, freq_dict[key]))

    return freq_dict


class DupFinder():
    def __init__(self, searchpaths):
        self.searchpaths = None
        self.master_root = None
        self.file_size_hash = None
        self.filetree = None
        self.filemodtimes = None
        self.fileblocks = None
        self.unproc_files = None
        self.dup_groups = None
        self.unique_files = None
        self.unique_dirs = None
        self.unknown_dirs = None

        # eliminate duplicates, and paths that are sub-paths of other
        #   searchpaths
        self._process_searchpaths(searchpaths)

    def _process_searchpaths(self, searchpaths):
        """Regularize searchpaths and remove redundants, get parent root of all.

        Convert all searchpaths to absolute paths.

        Remove duplicate searchpaths, or searchpaths contained completely
        inside another search path.

        Args:
            searchpaths: strings of search paths, relative or absolute

        Affects:
            self.master_root: string that is lowest common root dir for all
                searched files, dirs
            self.searchpaths: absolute paths, duplicates removed
        """
        remove_searchpath = {}

        # convert to absolute paths, getting real path, not linked dirs
        new_searchpaths = [os.path.realpath(x) for x in searchpaths]
        # eliminate duplicate paths
        new_searchpaths = list(set(new_searchpaths))
        # search for paths that are subdir of another path, eliminate them
        # TODO: this can be problemmatic if we eliminate one we need later?
        #       Maybe kick one path out and start over from the start?
        #       re-do this in general...
        for searchpath1 in new_searchpaths:
            for searchpath2 in new_searchpaths:
                test_relpath = os.path.relpath(searchpath1, start=searchpath2)
                if test_relpath != '.' and not test_relpath.startswith('..'):
                    # if '.' : searchpath1 and searchpath2 are same path
                    #   (search artifact)
                    # if relpath doesn't start with .. , then searchpath1 is
                    #   subdir of searchpath2
                    remove_searchpath[searchpath1] = True
        for (i, searchpath) in enumerate(new_searchpaths):
            if remove_searchpath.get(searchpath, False):
                del new_searchpaths[i]

        master_root = os.path.commonpath(new_searchpaths)
        self.master_root = master_root
        self.searchpaths = new_searchpaths

    def hash_files_by_size(self):
        """Hierarchically search through paths and has by file size in bytes

        Hierarchically traverse argument paths, and for every file make dict
        with keys being filesize in bytes and item being list of files
        that match

        Record heirarchical filetree containing dict of dicts structure
        mirroring dir/file hierarchy.

        Record the size in blocks into a dict for every file (keys are
        filepaths, items are size in blocks.)

        Record the modification time for every file (allowing us to check
        later if they changed during processing of this program.)

        Args:
            paths: search paths (each can be dir or file)
            master_root: string that is lowest common root dir for all
                searched files, dirs

        Returns:
            file_size_hash: key-size in bytes, item-list of files with that
                size
            filetree: dict of items and dicts corresponding to directory
                hierarchy of paths searched.  root of tree is master_root path
            filemodtimes: key-filepath, item-file modif. datetime
            fileblocks: key-filepath, item-size in blocks
            unproc_files: list of files ignored or unable to be read
        """

        unproc_files = []
        file_size_hash = {}
        filetree = {}
        fileblocks = {}
        filemodtimes = {}
        filesreport_time = time.time()

        #.........................
        # local function to process one file
        def process_file_size():
            """ Get size and otherwise catalog one file
            """
            # read/write these from hash_files_by_size scope
            nonlocal filesdone, filesreport_time

            filepath = os.path.join(root, filename)
            (this_size, this_mod, this_blocks, extra_info) = check_stat_file(
                    filepath)
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
            subtree_dict(filetree, root, self.master_root)[filename] = -1

            # setdefault returns [] if this_size key is not found
            # append as item to file_size_hash [filepath,filemodtime] to check if
            #   modified later
            file_size_hash.setdefault(this_size, []).append(filepath)
            filemodtimes[filepath] = this_mod

            filesdone += 1
            if filesdone%1000 == 0 or time.time()-filesreport_time > 15:
                myerr.print(
                        "\r  "+str(filesdone)+" files sized.", end='', flush=True)
                filesreport_time = time.time()
        #.........................

        # Actual hierarchical file stat processing
        for treeroot in self.searchpaths:
            # reset filesdone for each searchpath
            filesdone = 0
            myerr.print("Sizing: " + treeroot)
            # remove trailing slashes, etc.
            treeroot = os.path.normpath(treeroot)
            if os.path.isdir(treeroot):
                for (root, _, files) in os.walk(treeroot):
                    # TODO: get modtime on directories too, to see if they change?
                    for filename in files:
                        process_file_size()
            else:
                # this treeroot was a file
                (root, filename) = os.path.split(treeroot)
                process_file_size()

            # print final tally with CR
            myerr.print("\r  "+str(filesdone)+" files sized.")

        # tally unique, possibly duplicate files
        unique = 0
        nonunique = 0
        for key in file_size_hash:
            if len(file_size_hash[key]) == 1:
                unique += 1
            else:
                nonunique += len(file_size_hash[key])
        myerr.print("\nUnique: %d    "%unique)
        myerr.print("Possibly Non-Unique: %d\n"%nonunique)

        self.file_size_hash = file_size_hash
        self.filetree = filetree
        self.filemodtimes = filemodtimes
        self.fileblocks = fileblocks
        self.unproc_files = unproc_files

    def compare_files(self):
        """Determine duplicate, unique files from file data

        Each group of file_size_hash is a set of possible duplicate files (each
        group has files of all the same size in bytes.)  Read file data for
        each file in a group to determine which are ACTUALLY duplicate or
        unique files from the file data.

        Args:
            file_size_hash: key: size in bytes, item: list of files that size
            fileblocks: dict with key: filepath, item: file size in blocks
            unproc_files: READ/WRITE list of files that cannot be read, this
                list is added to by this function

        Returns:
            dup_groups: list of lists.  Each list contains:
                [size in blocks of duplicate files, list of duplicate files]
            unique_files: list of filepaths that are unique
        """
        unique_files = []
        dup_groups = []

        compare_files_timer = tictoc.Timer()
        compare_files_timer.start()
        myerr.print("Comparing file data...")

        old_time = 0
        for (i, key) in enumerate(self.file_size_hash.keys()):
            (this_unique_files, this_dup_groups, this_unproc_files
                    ) = compare_file_group(self.file_size_hash[key], self.fileblocks)
            unique_files.extend(this_unique_files)
            dup_groups.extend(this_dup_groups)
            self.unproc_files.extend(this_unproc_files)
            if compare_files_timer.eltime() > old_time+0.4:
                old_time = compare_files_timer.eltime()
                #compare_files_timer.eltime_pr("\rElapsed: ", end='', file=sys.stderr)
                compare_files_timer.progress_pr(
                        frac_done=(i+1)/len(self.file_size_hash),
                        file=sys.stderr
                        )
        # print one last time to get the 100% done tally
        compare_files_timer.progress_pr(
                frac_done=(i+1)/len(self.file_size_hash),
                file=sys.stderr
                )

        myerr.print("\nFinished comparing file data")

        self.dup_groups = dup_groups
        self.unique_files = unique_files

    def check_files_for_changes(self):
        """Look for files that have been modified during execution of this prog.

        Any change in a file since the beginning of this program's execution
        invalidates the uniqueness/duplicateness analysis of that file.
        It is removed from dup_groups or unique_files and placed in
        unproc_files with the tag "changed".

        Args:
            filemodtimes:
            unproc_files: READ/WRITE
            dup_groups: READ/WRITE
            unique_files: READ/WRITE
            filetree: READ/WRITE
            master_root:
        """
        for filepath in self.filemodtimes:
            (this_size, this_mod, this_blocks, extra_info) = check_stat_file(
                    filepath)
            if this_mod != self.filemodtimes[filepath]:
                # file has changed since start of this program
                (this_dir, this_file) = os.path.split(filepath)
                subtree_dict(self.filetree, this_dir, self.master_root)[this_file] = -1
                self.unproc_files.append([filepath, "changed"])

                # remove filepath from dups and unique if found
                if filepath in self.unique_files:
                    self.unique_files.remove(filepath)
                for dup_group in self.dup_groups:
                    if filepath in dup_group[1]:
                        dup_group[1].remove(filepath)


    def create_file_ids2(self):
        create_file_ids(
                self.dup_groups,
                self.unique_files,
                self.filetree,
                self.master_root
                )

    def recurse_analyze_filetree2(self):
        (self.unique_dirs, self.unknown_dirs) = recurse_analyze_filetree(
            self.filetree, self.master_root, self.fileblocks, self.dup_groups)

    def print_header2(self):
        print_header(self.master_root)
    
    def print_sorted_dups2(self):
        print_sorted_dups(self.dup_groups, self.master_root)

    def print_sorted_uniques2(self):
        unique_files = self.unique_files
        unique_files.extend(self.unique_dirs)
        print_sorted_uniques(unique_files, self.master_root)

    def print_unproc_files2(self):
        print_unproc_files(self.unproc_files, self.master_root)
    
    def print_unknown_dirs2(self):
        print_unknown_dirs(self.unknown_dirs, self.master_root)

#   1. For every file, get: size, mod_time
#   2. hash by sizes, each hash size in dict fill with list of all files
#       that size
#   3. go through each hash, deciding which of the list are unique or same
#       by comparing matching-size files chunk by chunk, splitting into
#       subgroups as differences found
def main(argv=None):
    """Search one or more searchpaths, report unique or duplicate files.

    Files are searched by data only, file names and attributes are
    irrelevant to determining uniqueness.

    In all internal data structures, paths are represented as absolute.

    In the report, paths are relative to the lowest common path of all
    searchpaths.

    Args:
        switches
        searchpaths
    """
    mytimer = tictoc.Timer()
    mytimer.start()
    args = process_command_line(argv)

    dup_find = DupFinder(args.searchpaths)

    # ANALYZE FILES, DIRECTORIES

    # file_size_hash is dict: keys are file sizes in bytes, items are lists of
    #   filepaths that are all that size (lists are all len > 1)
    # filemodtimes is dict: keys are filepaths, items are modtimes when file
    #   was stat'ed
    dup_find.hash_files_by_size()

    # DEBUG find frequencies of group sizes (collect statistics)
    #freq_dict = get_frequencies(file_size_hash)

    # compare all filegroups by using byte-by-byte comparison to find actually
    #   unique, duplicate
    # dupgroups: list of lists of identical files
    # uniquefiles: list unique files
    # unproc_files: list problematic (mostly unreadable) files
    dup_find.compare_files()

    # compare_files takes the longest time, so now check and see which files
    #   have changed in the meantime and mark them as changed
    # also mark in filetree as unprocessed, getting a -1 for their item in
    #   filetree hierarchical dict
    dup_find.check_files_for_changes()

    # now we know all of the files that are duplicates and unique
    # we will also determine below which directories are identical in that
    #   they contain identical files and subdirs

    # for each unique file, make a unique id, identical-data files share id
    #   save unique id to items of files in filetree hierarchical dict
    dup_find.create_file_ids2()

    # inventory directories based on identical/non-identical contents
    #   (ignoring names)
    # unknown_dirs are any dir that we couldn't check any of the files due to
    #   file issues, etc.
    # fileblocks gives info on file sizes, used to compute directory total size
    dup_find.recurse_analyze_filetree2()

    # PRINT REPORT

    # header for report
    dup_find.print_header2()

    # print a sorted (biggest dir/files first) list of dup groups,
    #   alphabetical within each group
    dup_find.print_sorted_dups2()

    # print a sorted (alphabetical) list of unique files and dirs
    dup_find.print_sorted_uniques2()

    # print lists of unprocessed files
    dup_find.print_unproc_files2()

    # print unknown status directories
    dup_find.print_unknown_dirs2()

    print("")
    mytimer.eltime_pr("Total Elapsed time: ", file=sys.stderr)
    mytimer.eltime_pr("Total Elapsed time: ", file=sys.stdout)
    return 0


if __name__ == '__main__':
    try:
        status = main(sys.argv)
    except KeyboardInterrupt:
        print("\nStopped by user.", file=sys.stderr)
        # "Terminated by Ctrl-C"
        status = 130
    sys.exit(status)
