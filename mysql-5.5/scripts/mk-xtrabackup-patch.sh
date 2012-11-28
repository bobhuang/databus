#! /bin/sh

XTRABACKUP_VER=1.6
ORIG_DIR=~/xtrabackup-$XTRABACKUP_VER
WORK_DIR=~/xtrabackup-build-$XTRABACKUP_VER/xtrabackup-$XTRABACKUP_VER
SVN_ROOT=~/svn-repo/mysql-5.5

if [ ! -d "$SVN_ROOT" ]
then
 SVN_ROOT=~/mysql-5.5
fi

PATCH_FILE=$SVN_ROOT/xtrabackup-patches/xtrabackup.patch

cd $WORK_DIR
diff -u $ORIG_DIR/xtrabackup.c xtrabackup.c > $PATCH_FILE 
