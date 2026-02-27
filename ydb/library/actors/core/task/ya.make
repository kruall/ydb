LIBRARY()

SRCS(
    task.h
    when_all.h
)

PEERDIR(
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
