LIBRARY()

SRCS(
    read.h
    read.cpp
)

PEERDIR(
    ydb/core/blobstorage/dsproxy
    ydb/library/actors/task
)

END()

RECURSE_FOR_TESTS(
    ut
)
