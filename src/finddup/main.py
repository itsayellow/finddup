#!/usr/bin/env python3

"""Find duplicate files, dirs based on their data, not names.
    Finds identical files, dirs even if they have different names.
    Searches hierarchically through all paths.
    Doesn't follow symbolic links
    Ignores: symbolic links, fifos, sockets, certain system info files
        like: .picasa.ini, .DS_Store, Thumbs.db, " Icon\r"
"""
# TODO: handle if searchpath is: a file, or nonexistent
# TODO: handle if no files in searchpaths
# TODO: handle if only one file
#   touch hi; finddup hi

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
import stat
import sys
import argparse
import time
import textwrap
from pathlib import Path
from typing import Tuple

import tictoc

from finddup.finddup import DupFinder


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
        description="Find duplicate files and directories in all paths.  "
        "Looks at file content, not names or info."
    )

    # specifying nargs= puts outputs of parser in list (even if nargs=1)

    # required arguments
    parser.add_argument(
        "searchpaths",
        nargs="+",
        metavar="searchpath",
        help="Search path(s) (recursively searched).",
    )

    # switches/options:
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Verbose status messages.",
    )

    # (settings, args) = parser.parse_args(argv)
    args = parser.parse_args(argv)

    return args


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

    search_paths = [Path(x) for x in args.searchpaths]
    # Make sure all searchpaths exist
    for search_path in search_paths:
        if not search_path.exists():
            print("Error: " + str(search_path) + " does not exist.", file=sys.stderr)
            return 1

    # initialize DupFinder object with searchpaths
    dup_find = DupFinder(search_paths)

    # ANALYZE FILES, DIRECTORIES
    dup_find.analyze()

    # PRINT REPORT
    dup_find.print_full_report()

    print("")
    mytimer.eltime_pr("Total Elapsed time: ", file=sys.stderr)
    mytimer.eltime_pr("Total Elapsed time: ", file=sys.stdout)
    return 0


def cli():
    try:
        status = main(sys.argv)
    except KeyboardInterrupt:
        print("\nStopped by user.", file=sys.stderr)
        # "Terminated by Ctrl-C"
        status = 130
    sys.exit(status)


if __name__ == "__main__":
    cli()
