LIBRARY()

SRCS(
    service_map_subsystem.h
    task.h
    task_benchmark_helper.h
    task_executor_actor.cpp
    task_system.h
)

PEERDIR(
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
