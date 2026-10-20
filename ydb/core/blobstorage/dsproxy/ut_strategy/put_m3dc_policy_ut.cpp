#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_put_m3dc_policy.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_put_m3dc.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_accelerate_put_m3dc.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

namespace {

using ESituation = TBlobState::ESituation;

struct TFixture {
    TIntrusivePtr<TBlobStorageGroupInfo> Info = MakeIntrusive<TBlobStorageGroupInfo>(
        TBlobStorageGroupType::ErasureMirror3dc, 1u, 3u, 3u);
    TIntrusivePtr<TGroupQueues> Queues = MakeIntrusive<TGroupQueues>(Info->GetTopology());
    TBlackboard Blackboard{Info, Queues, NKikimrBlobStorage::UserData, NKikimrBlobStorage::FastRead};
    TLogContext LogCtx{NKikimrServices::BS_PROXY, false};
    TBlobState Initial;

    TFixture() {
        Info->WritePolicy.emplace();
        Info->WritePolicy->WriteOk = {{1, 1, 1}, {2, 2}};
        Info->WritePolicy->WriteDurability = {{3, 3, 3}};
        Blackboard.AccelerationMode = TBlackboard::AccelerationModeSkipMarked;
        LogCtx.SuppressLog = true;
        Initial.Init(TLogoBlobID(1, 1, 1, 0, 1000, 0), *Info);
        Initial.BlobIdx = 0;
        for (ui32 part = 0; part < 3; ++part) {
            Initial.AddPartToPut(part, TRope(TString(1000, 'x')));
        }
    }

    void Compare(const TBlobState& state, TEvBlobStorage::TEvPut::ETactic tactic, bool mod3x3,
            bool accelerate) {
        auto oldState = state;
        auto newState = state;
        TGroupDiskRequests oldRequests;
        TGroupDiskRequests newRequests;
        TPut3dcStrategy oldStrategy(tactic, mod3x3);
        TAcceleratePut3dcStrategy oldAccelerate(tactic, mod3x3);
        IStrategy& legacy = accelerate ? static_cast<IStrategy&>(oldAccelerate) : oldStrategy;
        const auto oldResult = legacy.Process(LogCtx, oldState, *Info, Blackboard, oldRequests, {});
        TPut3dcPolicyStrategy strategy(tactic, mod3x3, accelerate);
        const auto result = strategy.Process(LogCtx, newState, *Info, Blackboard, newRequests, {});
        const auto context = [&] {
            return TStringBuilder() << "state# " << state.ToString()
                << " tactic# " << static_cast<ui32>(tactic) << " mod3x3# " << mod3x3 << " accelerate# " << accelerate;
        };
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(result.Value), static_cast<ui32>(oldResult.Value), context());
        UNIT_ASSERT_VALUES_EQUAL_C(newRequests.PutsPending.size(), oldRequests.PutsPending.size(), context());
        for (size_t i = 0; i < oldRequests.PutsPending.size(); ++i) {
            const auto& oldPut = oldRequests.PutsPending[i];
            const auto& put = newRequests.PutsPending[i];
            UNIT_ASSERT_VALUES_EQUAL_C(put.OrderNumber, oldPut.OrderNumber, context());
            UNIT_ASSERT_VALUES_EQUAL_C(put.Id, oldPut.Id, context());
            UNIT_ASSERT_VALUES_EQUAL_C(put.Buffer.ConvertToString(), oldPut.Buffer.ConvertToString(), context());
            UNIT_ASSERT_VALUES_EQUAL_C(put.IsHandoff, oldPut.IsHandoff, context());
        }
        for (ui32 disk = 0; disk < 9; ++disk) {
            UNIT_ASSERT_VALUES_EQUAL_C(newState.Disks[disk].IsSlow, oldState.Disks[disk].IsSlow, context());
            for (ui32 part = 0; part < 3; ++part) {
                UNIT_ASSERT_C(newState.Disks[disk].DiskParts[part].Situation
                    == oldState.Disks[disk].DiskParts[part].Situation, context());
            }
        }
    }

    EStrategyOutcome Run(TBlobState* state, TGroupDiskRequests* requests) {
        TPut3dcPolicyStrategy strategy(TEvBlobStorage::TEvPut::TacticDefault, false);
        return strategy.Process(LogCtx, *state, *Info, Blackboard, *requests, {});
    }
};

} // namespace

Y_UNIT_TEST_SUITE(DSProxyPut3dcPolicyTest) {
    Y_UNIT_TEST(LegacyEquivalenceAllDiskStates) {
        TFixture f;
        constexpr ESituation situations[] = {ESituation::Unknown, ESituation::Present, ESituation::Error, ESituation::Sent};
        for (ui32 encoded = 0; encoded < 262144; ++encoded) { // All 4^9 assignments.
            auto state = f.Initial;
            ui32 value = encoded;
            for (ui32 disk = 0; disk < 9; ++disk) {
                state.Disks[disk].DiskParts[disk % 3].Situation = situations[value % 4];
                value /= 4;
            }
            for (ui32 tactic = 0; tactic < TEvBlobStorage::TEvPut::TacticCount; ++tactic) {
                for (bool mod3x3 : {false, true}) {
                    for (bool strictDurability : {false, true}) {
                        f.Info->WritePolicy->WriteDurability = strictDurability
                            ? TBlobStorageGroupInfo::TWritePolicy::TLayouts{{3, 3, 3}}
                            : f.Info->WritePolicy->WriteOk;
                        f.Compare(state, static_cast<TEvBlobStorage::TEvPut::ETactic>(tactic), mod3x3, false);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(LegacyEquivalenceSlowAndInflight) {
        TFixture f;
        constexpr ESituation situations[] = {ESituation::Unknown, ESituation::Present, ESituation::Error, ESituation::Sent};
        for (ui32 slowMask = 0; slowMask < 512; ++slowMask) {
            for (ui32 disk = 0; disk < 9; ++disk) {
                auto& flow = f.Queues->DisksByOrderNumber[f.Initial.Disks[disk].OrderNumber]->Queues.PutUserData.FlowRecord;
                if (!flow) {
                    flow = MakeIntrusive<NBackpressure::TFlowRecord>();
                }
                flow->SetPredictedDelayNs(slowMask & (1 << disk) ? 1000 : 1);
            }
            for (ui32 pattern = 0; pattern < 8; ++pattern) {
                auto state = f.Initial;
                for (ui32 disk = 0; disk < 9; ++disk) {
                    const auto situation = [&] {
                        switch (pattern) {
                            case 0: return ESituation::Unknown;
                            case 1: return ESituation::Sent;
                            case 2: return disk % 3 == 0 ? ESituation::Error : ESituation::Unknown;
                            case 3: return disk < 3 ? ESituation::Present : ESituation::Sent;
                            case 4: return disk % 3 == 0 ? ESituation::Sent : ESituation::Unknown;
                            default: return situations[(disk + pattern) % 4];
                        }
                    }();
                    state.Disks[disk].DiskParts[disk % 3].Situation = situation;
                    state.Disks[disk].IsSlow = slowMask & (1 << disk);
                }
                for (ui32 tactic = 0; tactic < TEvBlobStorage::TEvPut::TacticCount; ++tactic) {
                    for (bool mod3x3 : {false, true}) {
                        for (bool accelerate : {false, true}) {
                            f.Compare(state, static_cast<TEvBlobStorage::TEvPut::ETactic>(tactic), mod3x3,
                                accelerate);
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(LegacyEquivalenceSpecificLayouts) {
        TFixture f;
        for (ui32 failedRealm = 0; failedRealm < 4; ++failedRealm) {
            const bool degraded = failedRealm < 3;
            f.Info->WritePolicy->WriteOk = degraded
                ? TBlobStorageGroupInfo::TWritePolicy::TLayouts{{2, 2}}
                : TBlobStorageGroupInfo::TWritePolicy::TLayouts{{1, 1, 1}};
            for (ui32 successMask = 0; successMask < (degraded ? 64u : 8u); ++successMask) {
                auto state = f.Initial;
                ui32 successBit = 0;
                for (ui32 disk = 0; disk < 9; ++disk) {
                    auto& situation = state.Disks[disk].DiskParts[disk % 3].Situation;
                    if (degraded && disk % 3 == failedRealm) {
                        situation = ESituation::Error;
                    } else if (degraded || disk < 3) {
                        // In the healthy case restrict progress to one copy per realm:
                        // otherwise the legacy OR condition can already finish on 2+2.
                        if (successMask & (1 << successBit)) {
                            situation = ESituation::Present;
                        }
                        ++successBit;
                    }
                }
                for (ui32 tactic = 0; tactic < TEvBlobStorage::TEvPut::TacticCount; ++tactic) {
                    for (bool mod3x3 : {false, true}) {
                        f.Compare(state, static_cast<TEvBlobStorage::TEvPut::ETactic>(tactic), mod3x3,
                            false);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(WriteDurabilityDoesNotAffectPlacement) {
        TFixture f;
        f.Info->WritePolicy->WriteOk = {{1, 1, 1}};
        f.Info->WritePolicy->WriteDurability = {{3, 3, 3}};
        auto state = f.Initial;
        TGroupDiskRequests requests;
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT_VALUES_EQUAL(requests.PutsPending.size(), 3);
        for (const auto& put : requests.PutsPending) {
            const ui32 subgroupIdx = f.Info->GetIdxInSubgroup(f.Info->GetVDiskId(put.OrderNumber), state.Id.Hash());
            UNIT_ASSERT(subgroupIdx < 9);
            UNIT_ASSERT_VALUES_EQUAL(put.Id.PartId() - 1, subgroupIdx % 3);
        }
        state = f.Initial;
        requests.PutsPending.clear();
        for (ui32 disk = 0; disk < 3; ++disk) {
            state.Disks[disk].DiskParts[disk % 3].Situation = ESituation::Present;
        }
        // WriteOk is sufficient even though WriteDurability still requires six copies.
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::DONE);
        UNIT_ASSERT(requests.PutsPending.empty());

        f.Info->WritePolicy->WriteOk = {{2, 2}};
        f.Info->WritePolicy->WriteDurability = {{1, 1, 1}};
        // Satisfying WriteDurability alone must not complete the client PUT.
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT_VALUES_EQUAL(requests.PutsPending.size(), 3);
        state = f.Initial;
        requests.PutsPending.clear();
        for (ui32 disk : {0u, 1u, 3u, 4u}) {
            state.Disks[disk].DiskParts[disk % 3].Situation = ESituation::Present;
        }
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::DONE);
        UNIT_ASSERT(requests.PutsPending.empty());
    }

    Y_UNIT_TEST(SentDoesNotSatisfyPolicy) {
        TFixture f;
        auto state = f.Initial;
        for (ui32 disk = 0; disk < 3; ++disk) {
            state.Disks[disk].DiskParts[disk % 3].Situation = ESituation::Sent;
        }
        TGroupDiskRequests requests;
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT(requests.PutsPending.empty());
    }

    Y_UNIT_TEST(RequiredRealmUnavailable) {
        TFixture f;
        f.Info->WritePolicy->WriteOk = {{1, 1, 1}};
        auto state = f.Initial;
        for (ui32 disk : {0u, 3u, 6u}) {
            state.Disks[disk].DiskParts[0].Situation = ESituation::Error;
        }
        TGroupDiskRequests requests;
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::ERROR);
        UNIT_ASSERT(requests.PutsPending.empty());
        f.Info->WritePolicy->WriteOk = {{2, 2}};
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT_VALUES_EQUAL(requests.PutsPending.size(), 4);
    }

    Y_UNIT_TEST(UnsupportedContextIsRejected) {
        TFixture f;
        auto state = f.Initial;
        TGroupDiskRequests requests;
        for (auto vdiskState : {TBlobStorageGroupInfo::EVDiskState::Excluded,
                TBlobStorageGroupInfo::EVDiskState::Replicating}) {
            f.Info->VDiskStates = {{0, vdiskState}, {1, vdiskState}, {2, vdiskState}};
            UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::ERROR);
        }
        f.Info->VDiskStates.clear();
        f.Info->WritePolicy.reset();
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::ERROR);
        UNIT_ASSERT(requests.PutsPending.empty());
    }
}
