set(EMSHEADERS 
    Util.h
    ExternalMergeSort.h
    )
    
set_source_files_properties(${EMSHEADERS} PROPERTIES HEADER_FILE_ONLY 1)
    
set(EMSSRC 
    Util.cpp
    ExternalMergeSort.cpp
    )
    
add_library(ems ${EMSSRC} ${EMSHEADERS})

install(TARGETS ems DESTINATION lib)
install(FILES ${EMSHEADERS} DESTINATION include)