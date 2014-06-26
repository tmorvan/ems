cmake_minimum_required(VERSION 2.8)

enable_testing()

project(ems)

#Enable c++11 support for GCC
if(CMAKE_COMPILER_IS_GNUCC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")
endif(CMAKE_COMPILER_IS_GNUCC)

include_directories(src)

add_subdirectory(src)
add_subdirectory(app)
add_subdirectory(test)
