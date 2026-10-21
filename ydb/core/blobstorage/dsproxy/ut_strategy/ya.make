UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/blobstorage/dsproxy
)

YQL_LAST_ABI_VERSION()

SRCS(
    strategy_ut.cpp
    put_m3dc_policy_ut.cpp
)

END()
