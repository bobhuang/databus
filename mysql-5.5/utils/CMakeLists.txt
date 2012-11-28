CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
STRING(REGEX REPLACE "/([a-zA-Z0-9_]|-)+/([a-zA-Z0-9_]|-)+$" "" 
  MYSQL_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${MYSQL_ROOT}/include)
#LINK_DIRECTORIES(${MYSQL_ROOT}/libmysql)
ADD_EXECUTABLE(multi-table-bench multi-table-bench.cc libbench.cc)
ADD_EXECUTABLE(fsync-bench fsync-bench.cc libbench.cc)
ADD_EXECUTABLE(tokenizer tokenizer.cc libbench.cc)
ADD_LIBRARY(mysqlclient STATIC IMPORTED)
SET_PROPERTY(TARGET mysqlclient PROPERTY IMPORTED_LOCATION
                 ${MYSQL_ROOT}/libmysql/libmysqlclient.a)
TARGET_LINK_LIBRARIES(multi-table-bench mysqlclient m rt dl pthread z)
TARGET_LINK_LIBRARIES(tokenizer mysqlclient m rt dl pthread z)
TARGET_LINK_LIBRARIES(fsync-bench mysqlclient m rt dl pthread z)
ADD_DEFINITIONS(-DHAVE_DLOPEN)

