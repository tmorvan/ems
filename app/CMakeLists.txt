set(SORTFILESRC 
    SortFile.cpp
    )
    
find_package(CUDA)
if(CUDA_FOUND)
  cuda_add_executable(sortfile ${SORTFILESRC} ${EMSHEADERS})
  include_directories(${CUDA_INCLUDE_DIRS})
  set_target_properties(sortfile PROPERTIES COMPILE_DEFINITIONS "WITH_CUDA")
else(CUDA_FOUND)
  add_executable(sortfile ${SORTFILESRC} ${EMSHEADERS})
endif(CUDA_FOUND)

install(TARGETS sortfile DESTINATION bin)
install(FILES ${EMSHEADERS} DESTINATION include)

set (CREATERANDOMFILESRC
    CreateRandomFile.cpp
    )
    
add_executable(createrandomfile ${CREATERANDOMFILESRC} ${EMSHEADERS})
    
install (TARGETS createrandomfile DESTINATION bin)

set (CHECKSORTEDFILESRC
    CheckSortedFile.cpp
    )
    
add_executable(checksortedfile ${CHECKSORTEDFILESRC} ${EMSHEADERS})
    
install (TARGETS checksortedfile DESTINATION bin)