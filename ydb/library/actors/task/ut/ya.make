UNITTEST_FOR(ydb/library/actors/task)

FORK_SUBTESTS()
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/library/actors/testlib
)

SRCS(
    task_ut.cpp
)

END()
