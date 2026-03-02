UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
        task_read.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/dsproxy/task
        ydb/core/blobstorage/ut_blobstorage/lib
        ydb/core/util/actorsys_test
        ydb/library/actors/task
    )

END()
