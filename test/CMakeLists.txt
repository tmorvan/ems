set(TESTPARAMSSRC 
    TestParams.cpp
    )
    
add_executable(testparams ${TESTPARAMSSRC})
target_link_libraries(testparams ems)

add_test(testparams testparams)

set(TESTSORTSRC
    TestSort.cpp
    )
    
add_executable(testsort ${TESTSORTSRC})
target_link_libraries(testsort ems)
    
add_test(testsort testsort)
