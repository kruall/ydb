UNITTEST_FOR(ydb/core/blobstorage/dsproxy/task)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/blobstorage/bridge/proxy
    ydb/core/blobstorage/dsproxy/task
    ydb/core/util/actorsys_test
)

SRCS(
    range_ut.cpp
    read_ut.cpp
)

END()
