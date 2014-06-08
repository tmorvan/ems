set(SORTFILESRC 
    SortFile.cpp
    )
    
add_executable(sortfile ${SORTFILESRC} ${EMSHEADERS})

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