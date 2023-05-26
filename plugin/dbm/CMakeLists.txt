SET(CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations ${CMAKE_CXX_FLAGS}")

MYSQL_ADD_PLUGIN(DBM dbm.cc plugin.cc DEFAULT)