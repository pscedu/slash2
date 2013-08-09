#!/bin/bash
################################################################################
#                                                                              #
#        Copyright (c) 2004, The Regents of the University of California       #
#      See the file COPYRIGHT for a complete copyright notice and license.     #
#                                                                              #
################################################################################
#
# FILE AND DIRECTORY TREE CREATION/REMOVAL TIMING TEST
# ********************************** WARNING ON ******************************
# BE VERY CAREFUL WITH THIS TEST AS IT CAN CREATE A VERY LARGE DIRECTORY TREE
# AND A GREAT DEAL OF DATA SIMPLY BY CHOOSING THE WRONG PARAMATERS.  STAY WITH
# THE DEFAULTS UNTIL YOU UNDERSTAND THE PARAMATERS...
# ********************************** WARNING OFF *****************************
#
# This script recursively creates level_depth directories below pathname with 
# files_per_dir files in every directory of size size_of_files.  
# Every directory except leaf directories then have dirs_per_level 
# subdirectories and so on.  Default starting point is "LEVEL0" directory in
# the current working directory.
# The directories are created first, by recursion and then the files in each
# directory are created by writing to them by another recursion.  
# Then the files are deleted by a nother recursion. 
# Finally the directories are removed by recursion.  
# Times for each operation are reported.
#
# Written by Mark K. Seager (seager@llnl.gov, 925-423-3141) and
#            Bill Loewe (wel@llnl.gov, 925-422-5587)
#
usage="fdtree.bash [-C] [-D] [-R] [-l level_depth] [-d dirs_per_level]
            [-f files_per_dir] [-s size_of_files (in file system blocks)]
            [-o pathname]"
#
# sample output
#serial#@KEROUAC c:/bench/io
#$ time fdtree.bash -f 3 -d5
#fdtree: starting at ./LEVEL0/
#        creating 4 directory levels with 5 directories at each level
#        for a total of 1081 directories
#        with 3 files of size 40KiB per directory
#        for a total of 3243 files and 12972KiB
#Wed Dec 18 20:15:24  2002
#Wed Dec 18 20:15:53  2002
#DIRECTORY CREATE TIME IN, OUT, TOTAL = 0, 29, 29
#        Directory creates per second =  37
#Wed Dec 18 20:15:53  2002
#Wed Dec 18 20:18:07  2002
#FILE CREATE TIME IN, OUT, TOTAL      = 29, 163, 134
#        File creates per second      =  24
#        KiB per second               =  96
#Wed Dec 18 20:18:07  2002
#Wed Dec 18 20:18:34  2002
#FILE REMOVE TIME IN, OUT, TOTAL      = 163, 190, 27
#        File removals per second     =  120
#Wed Dec 18 20:18:34  2002
#Wed Dec 18 20:18:52  2002
#DIRECTORY REMOVE TIME IN, OUT, TOTAL = 190, 208, 18
#        Directory removals per second =  60
#real 3m27.959s user 2m26.252s sys 1m41.627s CPU 100.00%

#############################################################################
# DEFINE FUNCTIONS
#############################################################################
function create_dirs
# this function creates the subdirectories and then
# recurses on each subdirectory until the bottom level is reached.
# create_level level base
# level is the level to create and base is the pathname of the starting point
{
    local nl
    local nf
    local nd
    local base_name
    local dir_name
    local file_name

    if [ $DEBUG -gt 0 ]
    then
	echo "entering create_dirs("$1,$2")"
    fi

    declare -i nl=$1
    declare -i nf=$files_per_dir
    declare -i nd=$dirs_per_level
    declare base_name=$2
    declare dir_name	


    # create subdirectories, if not in leaf dir
    nd=$dirs_per_level
    while [ $nd -gt 0 ] && [ $nl -gt 0 ] ; do
	if [ $DEBUG -gt 0 ]
	then
	    echo -e "\tcreating dir("$nd,$base_name"L"$nl"D"$nd"/"")"
	fi
	mkdir $base_name"L"$nl"D"$nd"/"
	let nd=$(($nd-1))
    done

    # recurse over subdirectories
    nd=$dirs_per_level
    while [ $nd -gt 0 ] && [ $nl -gt 0 ] ; do
	if [ $DEBUG -gt 0 ]
	then
	    echo -e "\trecursing create_dirs("$((nl-1)),$base_name"DIR"$nd"/"")"
	fi
	create_dirs $((nl-1)) $base_name"L"$nl"D"$nd"/"
	let nd=$(($nd-1))
    done
}

#############################################################################
function remove_dirs
# this function removes the subdirectories recursively
# remove_dirs level base
# level is the level to remove and base is the pathname of the starting point
{
    local nl
    local nf
    local nd
    local base_name
    local dir_names
    local file_name

    if [ $DEBUG -gt 0 ]
    then
	echo "entering remove_dirs("$1,$2")"
    fi

    declare -i nl=$1
    declare -i nf=$files_per_dir
    declare -i nd=$dirs_per_level
    declare base_name=$2
    declare dir_names	

    # recurse over subdirectories
    nd=$dirs_per_level
    while [ $nd -gt 0 ] && [ $nl -gt 1 ] ; do
	if [ $DEBUG -gt 0 ]
	then
	    echo -e "\trecursing remove_dirs("$((nl-1)),$base_name"DIR"$nd"/"")"
	fi
	remove_dirs $((nl-1)) $base_name"L"$nl"D"$nd"/"
	let nd=$(($nd-1))
    done
    # remove subdirectories, if one level above leaf directory
    if [ $nl -gt 0 ] 
    then
	nd=$dirs_per_level
	dir_names=""
	while [ $nd -gt 0 ] ; do
	    dir_names=$dir_names" "$base_name"L"$nl"D"$nd"/"
	    let nd=$(($nd-1))
	done
	if [ $DEBUG -gt 0 ]
	then
	    echo -e "\trmdir $dir_names"
	fi
	rmdir $dir_names
    fi
}

##############################################################################
function create_files
# this function creates files in this level and in each subdirectory by 
# recursion
{
    local nl
    local nf
    local nd
    local base_name
    local dir_name
    local file_name

    if [ $DEBUG -gt 0 ]
    then
	echo "entering create_files("$1,$2")"
    fi

    declare -i nl=$1
    declare -i nf=$files_per_dir
    declare -i nd=$dirs_per_level
    declare base_name=$2
    declare dir_name	


    # create files
    nf=$files_per_dir
    while [ $nf -gt 0 ] ; do
	file_name=$base_name"L"$nl"F"$nf
	if [ $DEBUG -gt 0 ]
	then
	    echo -e "\tcreating file("$nf,$file_name")"
	fi
	dd if=/dev/zero bs=4096 count=$fsize of=$file_name > /dev/null 2>&1
	let nf=$(($nf-1))
    done

    # recurse over subdirectories
    nd=$dirs_per_level
    while [ $nd -gt 0 ] && [ $nl -gt 0 ] ; do
	if [ $DEBUG -gt 0 ]
	then
	    echo -e "\trecursing create_files("$((nl-1)),$base_name"DIR"$nd"/"")"
	fi
	create_files $((nl-1)) $base_name"L"$nl"D"$nd"/"
	let nd=$(($nd-1))
    done
}

##############################################################################
function remove_files
# this function removes files in this level and in each subdirectory by 
# recursion
{
    local nl
    local nf
    local nd
    local base_name
    local dir_name
    local file_name

    if [ $DEBUG -gt 0 ]
    then
	echo "entering remove_files("$1,$2")"
    fi

    declare -i nl=$1
    declare -i nf=$files_per_dir
    declare -i nd=$dirs_per_level
    declare base_name=$2
    declare dir_name	

    # remove files
    nf=$files_per_dir
    file_name=""
    while [ $nf -gt 0 ] ; do
	file_name=$file_name" "$base_name"L"$nl"F"$nf
	let nf=$(($nf-1))
    done
    if [ $DEBUG -gt 0 ]
    then
	echo -e "\trm -f $file_name"
    fi
    rm -f $file_name

    # recurse over subdirectories
    nd=$dirs_per_level
    while [ $nd -gt 0 ] && [ $nl -gt 0 ] ; do
	if [ $DEBUG -gt 0 ]
	then
	    echo -e "\trecursing remove_files("$((nl-1)),$base_name"DIR"$nd"/"")"
	fi
	remove_files $((nl-1)) $base_name"L"$nl"D"$nd"/"
	let nd=$(($nd-1))
    done
}

##############################################################################
# set defaults for paramters and process command line options
##############################################################################
VERSION="1.0.2"			# release version
base="LEVEL0"			# start tree in CWD/LEVEL0
start_dir="."			# starting pathname
declare -i levels=4		# 4 levels in tree
declare -i dirs_per_level=10	# 10 subdirectories in each directory (fanout)
declare -i files_per_dir=10	# 10 files in each directory
declare -i fsize=10		# size of files in file system blocks (4096B)
declare -i DEBUG=0		# DEBUG FLAG.  Default is off
declare -i CREATE_TREE=0	# Create tree flag
declare -i REMOVE_TREE=0	# Remove tree flag
OPTIND=0			# initialize to 0 before getopts call

while getopts ":l:d:f:s:o:CDR" opt; do
    case $opt in
	l ) levels=$OPTARG ;;
	d ) dirs_per_level=$OPTARG ;;
	f ) files_per_dir=$OPTARG ;;
	s ) fsize=$OPTARG ;;
	o ) start_dir=$OPTARG ;;
	C ) CREATE_TREE=1 ;;
	D ) DEBUG=1 ;;
	R ) REMOVE_TREE=1 ;;
	\? ) IFS=''
	     echo $usage
	     IFS='\ \t\n'
	     echo -e "\t-C create tree only"
	     echo -e "\t-D turns on debugging"
	     echo -e "\t-R remove tree only"
	     echo -e "\t-l is the number of recursive levels below the root node"
	     echo -e "\t-d is the number of directories to create per level"
	     echo -e "\t-f is the number of files to create per directory"
	     echo -e "\t-o is the starting directory pathname"
	     echo -e "\t-s is the file size (in blocks, 4096 for Linux, to create)"
	     echo -e ""
	     echo "WARNING: directories and files created increases polynomically with -l."
	     echo -e "Be careful, very careful.  Your filesystem may suffer..."
	     echo -e ""
	     echo "EXAMPLE: fdtree.bash -d 1 -l 2 -f 10000 -s 10000"
	     echo -e "\tThis example is a file stress test and creates two levels with one"
	     echo -e "\tdirectory each and 10K files in each directory of size 10K*4K blocks."
	     echo -e "\tThis is 40,960,000B = 40.96 MB of data per file or 2*409,600,000,000B"
	     echo -e "\t= 819.2 GB of data.  You should not attempt this at home..."
	     echo -e ""
	     echo "EXAMPLE: fdtree.bash -d 10000 -l 100"
	     echo -e "\tThis is a directory stress test and creates 100 levels with 10K"
	     echo -e "\tdirectories per level."
	     echo -e "\tThis is 10K directories in the first level and (10K)*(10K)"
	     echo -e "\tdirectories in the second level and (10K)*(10K)*(10K) directories"
	     echo -e "\tin the third level and so on.  10K**100 = 10**500 directories at"
	     echo -e "\tthe 100th level.  You should not attempt this at home or work..."
	     exit 1 ;;
    esac
done

# set base directory -- hostname and process id are appended for parallelization purposes
base=$start_dir"/"$base"."`hostname -s`"."$$"/"

# if neither create nor remove are set, do both
if [ $CREATE_TREE -eq $REMOVE_TREE ]
then
    CREATE_TREE=1
    REMOVE_TREE=1
fi

# compute the number of directories that will be created.
declare -i nl=0
declare -i ndtot=0
while [ $nl -le $levels ] ; do
    ndtot=$((1+$dirs_per_level*$ndtot))
    nl=$(($nl+1))
    if [ $DEBUG -gt 0 ]
    then
	echo "NDTOT ="$ndtot, "Level ="$nl
    fi
done

# compute the number of files that will be created.
declare -i nftot=ndtot*files_per_dir

echo "fdtree-"$VERSION": starting at $base"
echo -e "\tcreating/deleting $levels directory levels with $dirs_per_level directories at each level"
echo -e "\tfor a total of $ndtot directories"
echo -e "\twith $files_per_dir files of size $((fsize*4))KiB per directory"
echo -e "\tfor a total of $nftot files and $((4*nftot*fsize))KiB"

if [ $CREATE_TREE -gt 0 ]
then
    ############################################################################
    # create the subdirectories by recursion
    ############################################################################
    date
    declare -i tin=$SECONDS
    mkdir $base
    if [ $DEBUG -gt 0 ]
    then
        echo "create_dirs("$levels,$base")"
    fi
    create_dirs $levels $base
    declare -i tout=$SECONDS
    declare -i ttot=tout-tin
    if [ $ttot -eq 0 ]
    then
        declare -i results=0
    else
        declare -i results=$ndtot/$ttot
    fi
    date
    echo "DIRECTORY CREATE TIME IN, OUT, TOTAL = "$tin, $tout, $ttot
    echo -e "\tDirectory creates per second = " $results
    
    ############################################################################
    # create the files in each subdirectory by recursion
    ############################################################################
    date
    declare -i tin=$SECONDS
    if [ $DEBUG -gt 0 ]
    then
        echo "create_files("$levels,$base")"
    fi
    create_files $levels $base
    declare -i tout=$SECONDS
    declare -i ttot=tout-tin
    if [ $ttot -eq 0 ]
    then
        declare -i results=0
        declare -i results2=0
    else
        declare -i results=$nftot/$ttot
        declare -i results2=$fsize*$nftot*4/$ttot
    fi
    date
    echo "FILE CREATE TIME IN, OUT, TOTAL      = "$tin, $tout, $ttot
    echo -e "\tFile creates per second      = " $results
    echo -e "\tKiB per second               = " $results2
fi
    
if [ $REMOVE_TREE -gt 0 ]
then
    ############################################################################
    # remove the files in each subdirectory by recursion
    ############################################################################
    date
    declare -i tin=$SECONDS
    if [ $DEBUG -gt 0 ]
    then
        echo "remove_files("$levels,$base")"
    fi
    remove_files $levels $base
    declare -i tout=$SECONDS
    declare -i ttot=tout-tin
    if [ $ttot -eq 0 ]
    then
        declare -i results=0
    else
        declare -i results=$nftot/$ttot
    fi
    date
    echo "FILE REMOVE TIME IN, OUT, TOTAL      = "$tin, $tout, $ttot
    echo -e "\tFile removals per second     = " $results
    
    ############################################################################
    # remove the subdirectories by recursion
    ############################################################################
    date
    declare -i tin=$SECONDS
    if [ $DEBUG -gt 0 ]
    then
        echo "remove_dirs("$levels,$base")"
    fi
    remove_dirs $levels $base
    rmdir $base
    declare -i tout=$SECONDS
    declare -i ttot=tout-tin
    if [ $ttot -eq 0 ]
    then
        declare -i results=0
    else
        declare -i results=$ndtot/$ttot
    fi
    date
    echo "DIRECTORY REMOVE TIME IN, OUT, TOTAL = "$tin, $tout, $ttot
    echo -e "\tDirectory removals per second = " $results
fi
