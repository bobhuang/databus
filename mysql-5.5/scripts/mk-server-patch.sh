#! /bin/sh

MYSQL_VER=5.5.8
ORIG_DIR=~/mysql-$MYSQL_VER
WORK_DIR=~/mysql-build-$MYSQL_VER/mysql-$MYSQL_VER
SVN_ROOT=~/svn-repo/mysql-5.5

if [ ! -d "$SVN_ROOT" ]
then
 SVN_ROOT=~/mysql-5.5
fi

PATCH_FILE=$SVN_ROOT/patches/server.patch

cd $WORK_DIR
diff -ruN -x \*.o -x Makefile -x \*CMake\*  -x \*cmake\* -x \*.so \
 -x \*.sys -x mysqld -x gen_lex_hash -x Makefile -x \*.a  -x mysql_tzinfo_to_sql \
 -x lex_hash.h -x sql_builtin.cc -x sql_yacc.cc -x sql_yacc.h \
  $ORIG_DIR/sql sql > $PATCH_FILE
diff -u $ORIG_DIR/sql/CMakeLists.txt sql/CMakeLists.txt >> $PATCH_FILE 
diff -u $ORIG_DIR/libmysqld/CMakeLists.txt libmysqld/CMakeLists.txt >> $PATCH_FILE 
diff -u $ORIG_DIR/support-files/CMakeLists.txt support-files/CMakeLists.txt >> $PATCH_FILE 
diff -u $ORIG_DIR/libmysql/CMakeLists.txt libmysql/CMakeLists.txt >> $PATCH_FILE 
