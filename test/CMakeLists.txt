set(TESTPARAMSSRC 
    TestParams.cpp
    )
    
add_executable(testparams ${TESTPARAMSSRC} ${EMSHEADERS})

add_test(testparams testparams)

set(TESTSORTSRC
    TestSort.cpp
    )
    
add_executable(testsort ${TESTSORTSRC} ${EMSHEADERS})
    
add_test(testsort testsort)


set(TESTTHREADPOOLSRC
    TestThreadPool.cpp
    )
    
add_executable(testthreadpool ${TESTTHREADPOOLSRC} ${EMSHEADERS})
    
add_test(testthreadpool testthreadpool)