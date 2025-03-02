UNITTEST()

FORK_SUBTESTS(MODULO)

SPLIT_FACTOR(24)

IF (SANITIZER_TYPE OR WITH_VALGRIND OR BUILD_TYPE == "DEBUG")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/ut_vdisk/lib
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    dsproxy_fault_tolerance_ut.cpp
)

END()
