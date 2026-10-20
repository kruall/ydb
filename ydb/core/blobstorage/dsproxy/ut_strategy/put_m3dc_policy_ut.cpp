#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_put_m3dc_policy.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_put_m3dc.h>

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

    std::array<ui32, 3> PendingCounts(const TGroupDiskRequests& requests) const {
        std::array<ui32, 3> counts{};
        for (const auto& put : requests.PutsPending) {
            const ui32 idx = Info->GetIdxInSubgroup(Info->GetVDiskId(put.OrderNumber), Initial.Id.Hash());
            UNIT_ASSERT(idx < 9);
            UNIT_ASSERT_VALUES_EQUAL(put.Id.PartId() - 1, idx % 3);
            ++counts[idx % 3];
        }
        return counts;
    }

    void CheckCounts(const TGroupDiskRequests& requests, const std::array<ui32, 3>& expected) const {
        const auto counts = PendingCounts(requests);
        for (ui32 realm = 0; realm < 3; ++realm) {
            UNIT_ASSERT_VALUES_EQUAL(counts[realm], expected[realm]);
        }
    }

    EStrategyOutcome Run(TBlobState* state, TGroupDiskRequests* requests,
            TEvBlobStorage::TEvPut::ETactic tactic = TEvBlobStorage::TEvPut::TacticDefault,
            bool accelerate = false) {
        TPut3dcPolicyStrategy strategy(tactic, accelerate);
        return strategy.Process(LogCtx, *state, *Info, Blackboard, *requests, {});
    }

    void CompareCompletion(const TBlobState& state, TEvBlobStorage::TEvPut::ETactic tactic) {
        auto oldState = state;
        auto newState = state;
        TGroupDiskRequests oldRequests;
        TGroupDiskRequests requests;
        TPut3dcStrategy oldStrategy(tactic, false);
        const auto oldResult = oldStrategy.Process(LogCtx, oldState, *Info, Blackboard, oldRequests, {});
        const auto result = Run(&newState, &requests, tactic);
        const auto context = [&] {
            return TStringBuilder() << "state# " << state.ToString()
                << " tactic# " << static_cast<ui32>(tactic);
        };
        UNIT_ASSERT_VALUES_EQUAL_C(static_cast<ui32>(result.Value), static_cast<ui32>(oldResult.Value), context());
        ui32 seen = 0;
        auto planned = PendingCounts(requests);
        for (const auto& put : requests.PutsPending) {
            const ui32 idx = Info->GetIdxInSubgroup(Info->GetVDiskId(put.OrderNumber), state.Id.Hash());
            const auto before = state.Disks[idx].DiskParts[idx % 3].Situation;
            UNIT_ASSERT_C(before == ESituation::Unknown || before == ESituation::Absent || before == ESituation::Lost, context());
            UNIT_ASSERT_C(!(seen & (1u << idx)), context());
            seen |= 1u << idx;
        }
        for (ui32 idx = 0; idx < 9; ++idx) {
            const auto situation = state.Disks[idx].DiskParts[idx % 3].Situation;
            if (situation == ESituation::Present || situation == ESituation::Sent) {
                ++planned[idx % 3];
            }
        }
        if (result.Value == EStrategyOutcome::IN_PROGRESS) {
            UNIT_ASSERT_C(Info->WritePolicy->CheckWriteOk(TVector<ui32>(planned.begin(), planned.end())), context());
        }
    }

    void SetSlow(TBlobState* state, ui32 mask) {
        for (ui32 idx = 0; idx < 9; ++idx) {
            auto& flow = Queues->DisksByOrderNumber[state->Disks[idx].OrderNumber]->Queues.PutUserData.FlowRecord;
            if (!flow) {
                flow = MakeIntrusive<NBackpressure::TFlowRecord>();
            }
            flow->SetPredictedDelayNs(mask & (1u << idx) ? 1000 : 1);
            state->Disks[idx].IsSlow = mask & (1u << idx);
        }
    }

};

} // namespace

Y_UNIT_TEST_SUITE(DSProxyPut3dcPolicyTest) {
    Y_UNIT_TEST(LegacyCompletionScenarios) {
        struct TScenario {
            ui32 Present;
            ui32 Error;
            ui32 Sent;
            ui32 Absent = 0;
            ui32 Lost = 0;
        };
        const TScenario cases[] = {
            {0, 0, 0}, // No replies.
            {0b000000001, 0, 0},
            {0b000000111, 0, 0}, // 1+1+1 confirmed.
            {0b000011011, 0, 0}, // 2+2 confirmed.
            {0, 0b001001001, 0}, // One failed realm.
            {0, 0b001001011, 0}, // One failed realm and one other disk.
            {0, 0b011011011, 0}, // Two failed realms.
            {0, 0b000000111, 0}, // Errors in all three realms.
            {0, 0, 0b111111111},
            {0b000001001, 0b100000000, 0b000010010},
            {0, 0, 0, 0b111111111},
            {0, 0, 0, 0, 0b111111111},
        };
        TFixture f;
        for (const auto& scenario : cases) {
            for (ui32 realmShift = 0; realmShift < 3; ++realmShift) {
                auto state = f.Initial;
                for (ui32 idx = 0; idx < 9; ++idx) {
                    const ui32 shifted = idx / 3 * 3 + (idx % 3 + realmShift) % 3;
                    auto& situation = state.Disks[shifted].DiskParts[shifted % 3].Situation;
                    const ui32 bit = 1u << idx;
                    if (scenario.Present & bit) {
                        situation = ESituation::Present;
                    } else if (scenario.Error & bit) {
                        situation = ESituation::Error;
                    } else if (scenario.Sent & bit) {
                        situation = ESituation::Sent;
                    } else if (scenario.Absent & bit) {
                        situation = ESituation::Absent;
                    } else if (scenario.Lost & bit) {
                        situation = ESituation::Lost;
                    }
                }
                for (ui32 tactic = 0; tactic < TEvBlobStorage::TEvPut::TacticCount; ++tactic) {
                    f.CompareCompletion(state, static_cast<TEvBlobStorage::TEvPut::ETactic>(tactic));
                }
            }
        }
    }

    Y_UNIT_TEST(ThroughputSelectsSpreadThenPartCount) {
        using TLayouts = TBlobStorageGroupInfo::TWritePolicy::TLayouts;
        const std::pair<TLayouts, std::array<ui32, 3>> cases[] = {
            {{{1, 1, 1}, {2, 2}}, {1, 1, 1}},
            {{{2, 2}}, {2, 2, 0}},
            {{{2, 2}, {2, 1}}, {2, 1, 0}},
            {{{2}, {1, 1}}, {1, 1, 0}},
            {{{2}, {2, 1}}, {2, 1, 0}},
        };
        for (const auto& [layouts, counts] : cases) {
            for (auto tactic : {TEvBlobStorage::TEvPut::TacticDefault, TEvBlobStorage::TEvPut::TacticMaxThroughput}) {
                TFixture f;
                f.Info->WritePolicy->WriteOk = layouts;
                auto state = f.Initial;
                TGroupDiskRequests requests;
                UNIT_ASSERT(f.Run(&state, &requests, tactic).Value == EStrategyOutcome::IN_PROGRESS);
                f.CheckCounts(requests, counts);
            }
        }
    }

    Y_UNIT_TEST(MinLatencyRectanglesAndExpansion) {
        using TLayouts = TBlobStorageGroupInfo::TWritePolicy::TLayouts;
        const std::pair<TLayouts, std::array<ui32, 3>> cases[] = {
            {{{1, 1, 1}, {2, 2}}, {2, 2, 2}}, // Bounding rectangle, no exact alternative.
            {{{2, 1}}, {2, 2, 0}},
            {{{2, 2}}, {2, 2, 2}}, // Equal cost: expand to more realms.
            {{{2}}, {3, 0, 0}}, // One extra copy costs less than another realm.
            {{{1}}, {1, 1, 0}},
            {{{3, 3, 3}}, {3, 3, 3}}, // No coordinate can be expanded.
        };
        for (const auto& [layouts, counts] : cases) {
            TFixture f;
            f.Info->WritePolicy->WriteOk = layouts;
            auto state = f.Initial;
            TGroupDiskRequests requests;
            UNIT_ASSERT(f.Run(&state, &requests, TEvBlobStorage::TEvPut::TacticMinLatency).Value == EStrategyOutcome::IN_PROGRESS);
            f.CheckCounts(requests, counts);
        }
    }

    Y_UNIT_TEST(MinLatencyExpansionAccountsForErrorsAndPresent) {
        TFixture f;
        f.Info->WritePolicy->WriteOk = {{2, 2}};
        auto state = f.Initial;
        state.Disks[8].DiskParts[2].Situation = ESituation::Error;
        state.Disks[0].DiskParts[0].Situation = ESituation::Present;
        state.Disks[1].DiskParts[1].Situation = ESituation::Present;
        TGroupDiskRequests requests;
        UNIT_ASSERT(f.Run(&state, &requests, TEvBlobStorage::TEvPut::TacticMinLatency).Value == EStrategyOutcome::IN_PROGRESS);
        f.CheckCounts(requests, {2, 2, 0}); // Equal cost: fewer errors beats a third realm.

        state = f.Initial;
        requests.PutsPending.clear();
        for (ui32 idx : {0u, 3u, 6u, 1u}) {
            state.Disks[idx].DiskParts[idx % 3].Situation = ESituation::Present;
        }
        UNIT_ASSERT(f.Run(&state, &requests, TEvBlobStorage::TEvPut::TacticMinLatency).Value == EStrategyOutcome::IN_PROGRESS);
        f.CheckCounts(requests, {0, 2, 0}); // Reuse Present when comparing expansion costs.

        state = f.Initial;
        requests.PutsPending.clear();
        for (ui32 idx : {0u, 3u, 1u, 2u}) {
            state.Disks[idx].DiskParts[idx % 3].Situation = ESituation::Present;
        }
        state.Disks[8].DiskParts[2].Situation = ESituation::Error;
        UNIT_ASSERT(f.Run(&state, &requests, TEvBlobStorage::TEvPut::TacticMinLatency).Value == EStrategyOutcome::IN_PROGRESS);
        f.CheckCounts(requests, {0, 1, 1}); // Lower cost precedes the error-count tie breaker.
    }

    Y_UNIT_TEST(UnreachableAlternativesAndPartialRectangles) {
        TFixture f;
        f.Info->WritePolicy->WriteOk = {{3}, {2, 2}};
        auto state = f.Initial;
        for (ui32 idx : {2u, 5u, 8u, 7u}) {
            state.Disks[idx].DiskParts[idx % 3].Situation = ESituation::Error;
        }
        TGroupDiskRequests requests;
        UNIT_ASSERT(f.Run(&state, &requests, TEvBlobStorage::TEvPut::TacticMinLatency).Value == EStrategyOutcome::IN_PROGRESS);
        f.CheckCounts(requests, {3, 2, 0});
        f.Info->WritePolicy->WriteOk = {{3, 3}, {2, 2}};
        state = f.Initial;
        requests.PutsPending.clear();
        for (ui32 idx : {2u, 5u, 8u, 7u}) {
            state.Disks[idx].DiskParts[idx % 3].Situation = ESituation::Error;
        }
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::IN_PROGRESS);
        f.CheckCounts(requests, {2, 2, 0});
    }

    Y_UNIT_TEST(SentDoesNotReducePlacementCost) {
        TFixture f;
        f.Info->WritePolicy->WriteOk = {{2}};
        auto state = f.Initial;
        state.Disks[0].DiskParts[0].Situation = ESituation::Sent;
        state.Disks[3].DiskParts[0].Situation = ESituation::Sent;
        state.Disks[1].DiskParts[1].Situation = ESituation::Present;
        TGroupDiskRequests requests;
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::IN_PROGRESS);
        f.CheckCounts(requests, {0, 1, 0});
    }

    Y_UNIT_TEST(SlowDisksFallbackAndAcceleration) {
        TFixture f;
        auto state = f.Initial;
        f.SetSlow(&state, 7);
        TGroupDiskRequests requests;
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::IN_PROGRESS);
        f.CheckCounts(requests, {1, 1, 1});
        for (const auto& put : requests.PutsPending) {
            const ui32 idx = f.Info->GetIdxInSubgroup(f.Info->GetVDiskId(put.OrderNumber), state.Id.Hash());
            UNIT_ASSERT(idx >= 3);
        }
        state = f.Initial;
        requests.PutsPending.clear();
        f.SetSlow(&state, 511);
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::IN_PROGRESS);
        f.CheckCounts(requests, {1, 1, 1});
        state = f.Initial;
        requests.PutsPending.clear();
        f.SetSlow(&state, 7);
        for (ui32 idx = 0; idx < 3; ++idx) {
            state.Disks[idx].DiskParts[idx].Situation = ESituation::Sent;
        }
        UNIT_ASSERT(f.Run(&state, &requests, TEvBlobStorage::TEvPut::TacticDefault, true).Value == EStrategyOutcome::IN_PROGRESS);
        f.CheckCounts(requests, {1, 1, 1});
        for (const auto& put : requests.PutsPending) {
            const ui32 idx = f.Info->GetIdxInSubgroup(f.Info->GetVDiskId(put.OrderNumber), state.Id.Hash());
            UNIT_ASSERT(idx >= 3);
        }
    }

    Y_UNIT_TEST(AllRealmCapacities) {
        using TLayouts = TBlobStorageGroupInfo::TWritePolicy::TLayouts;
        const TLayouts policies[] = {
            {{1}}, {{2}}, {{3}}, {{1, 1}}, {{2, 1}}, {{2, 2}}, {{3, 3}},
            {{1, 1, 1}}, {{2, 1, 1}}, {{2, 2, 2}}, {{3, 3, 3}},
            {{3}, {2, 2}}, {{1, 1, 1}, {2, 2}},
        };
        const auto fits = [](const TVector<ui32>& layout, std::array<ui32, 3> counts) {
            std::sort(counts.begin(), counts.end(), std::greater<ui32>());
            for (ui32 i = 0; i < layout.size(); ++i) {
                if (counts[i] < layout[i]) {
                    return false;
                }
            }
            return true;
        };
        for (ui32 encoded = 0; encoded < 64; ++encoded) {
            TFixture f;
            auto initial = f.Initial;
            std::array<ui32, 3> available{};
            auto errors = TBlobStorageGroupInfo::TSubgroupVDisks(&f.Info->GetTopology());
            ui32 value = encoded;
            for (ui32 realm = 0; realm < 3; ++realm) {
                available[realm] = value % 4;
                value /= 4;
                for (ui32 domain = available[realm]; domain < 3; ++domain) {
                    const ui32 idx = realm + domain * 3;
                    initial.Disks[idx].DiskParts[realm].Situation = ESituation::Error;
                    errors += TBlobStorageGroupInfo::TSubgroupVDisks(&f.Info->GetTopology(), idx);
                }
            }
            for (const auto& policy : policies) {
                f.Info->WritePolicy->WriteOk = policy;
                ui32 bestRealms = 0;
                ui32 bestParts = 10;
                for (const auto& layout : policy) {
                    if (fits(layout, available)) {
                        ui32 parts = 0;
                        for (ui32 count : layout) {
                            parts += count;
                        }
                        if (layout.size() > bestRealms) {
                            bestRealms = layout.size();
                            bestParts = parts;
                        } else if (layout.size() == bestRealms) {
                            bestParts = Min(bestParts, parts);
                        }
                    }
                }
                for (ui32 tactic = 0; tactic < TEvBlobStorage::TEvPut::TacticCount; ++tactic) {
                    auto state = initial;
                    TGroupDiskRequests requests;
                    const auto result = f.Run(&state, &requests, static_cast<TEvBlobStorage::TEvPut::ETactic>(tactic));
                    if (!bestRealms || !f.Info->GetQuorumChecker().CheckFailModelForSubgroup(errors)) {
                        UNIT_ASSERT(result.Value == EStrategyOutcome::ERROR);
                        UNIT_ASSERT(requests.PutsPending.empty());
                        continue;
                    }
                    UNIT_ASSERT(result.Value == EStrategyOutcome::IN_PROGRESS);
                    const auto counts = f.PendingCounts(requests);
                    if (tactic == TEvBlobStorage::TEvPut::TacticMinLatency) {
                        for (const auto& layout : policy) {
                            if (fits(layout, available)) {
                                UNIT_ASSERT(fits(layout, counts));
                            }
                        }
                    } else {
                        ui32 realms = 0;
                        ui32 parts = 0;
                        for (ui32 count : counts) {
                            realms += count != 0;
                            parts += count;
                        }
                        UNIT_ASSERT_VALUES_EQUAL(realms, bestRealms);
                        UNIT_ASSERT_VALUES_EQUAL(parts, bestParts);
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
        // WriteOk is sufficient even though WriteDurability requires six more copies.
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::DONE);
        UNIT_ASSERT(requests.PutsPending.empty());

        f.Info->WritePolicy->WriteOk = {{2, 2}};
        f.Info->WritePolicy->WriteDurability = {{1, 1, 1}};
        // Satisfying WriteDurability alone must not complete the client PUT.
        UNIT_ASSERT(f.Run(&state, &requests).Value == EStrategyOutcome::IN_PROGRESS);
        UNIT_ASSERT_VALUES_EQUAL(requests.PutsPending.size(), 2);
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
