LIBRARY()

SRCS(
    task_executor_actor.cpp
    task_system.h
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
