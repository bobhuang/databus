#! /bin/sh

MYSQL_VER=5.5.8
ORIG_DIR=~/mysql-$MYSQL_VER
WORK_DIR=~/mysql-build-$MYSQL_VER/mysql-$MYSQL_VER
SVN_ROOT=~/svn-repo/mysql-5.5

if [ ! -d "$SVN_ROOT" ]
then
 SVN_ROOT=~/mysql-5.5
fi

PATCH_FILE=$SVN_ROOT/patches/innodb.patch

cd $ORIG_DIR
diff -ruN -x \*.o -x Makefile -x \*CMake\*  -x \*cmake\* storage/innobase/ $WORK_DIR/storage/innobase  > $SVN_ROOT/patches/innodb.patch
