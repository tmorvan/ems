cmake_minimum_required(VERSION 2.8)
enable_testing()

project(ems)

include_directories(src)

add_subdirectory(src)
add_subdirectory(app)
add_subdirectory(test)
