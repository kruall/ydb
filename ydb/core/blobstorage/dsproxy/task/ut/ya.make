UNITTEST_FOR(ydb/core/blobstorage/dsproxy/task)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/blobstorage/dsproxy/task
    ydb/core/util/actorsys_test
)

SRCS(
    read_ut.cpp
)

END()
