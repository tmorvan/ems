set(EMSHEADERS 
    ${CMAKE_CURRENT_SOURCE_DIR}/Util.h
    ${CMAKE_CURRENT_SOURCE_DIR}/Util-inl.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ThreadPool.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ThreadPool-inl.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ExternalMergeSortBase.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ExternalMergeSort.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ExternalMergeSort-inl.h
    PARENT_SCOPE
    )
    
set_source_files_properties(${EMSHEADERS} PROPERTIES HEADER_FILE_ONLY 1)