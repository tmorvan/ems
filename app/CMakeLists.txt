set(SORTFILESRC 
    SortFile.cpp
    )
    
add_executable(sortfile ${SORTFILESRC})
target_link_libraries(sortfile ems)

install(TARGETS sortfile DESTINATION bin)

set (CREATERANDOMFILESRC
    CreateRandomFile.cpp
    )
    
add_executable(createrandomfile ${CREATERANDOMFILESRC})
target_link_libraries(createrandomfile ems)
    
install (TARGETS createrandomfile DESTINATION bin)

set (CHECKSORTEDFILESRC
    CheckSortedFile.cpp
    )
    
add_executable(checksortedfile ${CHECKSORTEDFILESRC})
target_link_libraries(checksortedfile ems)
    
install (TARGETS checksortedfile DESTINATION bin)