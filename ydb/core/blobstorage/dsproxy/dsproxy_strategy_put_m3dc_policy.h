#pragma once

#include "dsproxy_strategy_base.h"

namespace NKikimr {

// A placement strategy only: acknowledging WriteOk before WriteDurability requires
// a separate change to the PUT request's completion and lifetime handling.
class TPut3dcPolicyStrategy : public TStrategyBase {
public:
    enum class ETarget {
        WriteOk,
        WriteDurability,
    };

private:
    const TEvBlobStorage::TEvPut::ETactic Tactic;
    const bool EnableRequestMod3x3ForMinLatecy;
    const ETarget Target;
    const bool Accelerate;

    bool CheckLayout(const TBlobStorageGroupInfo& info,
            const TBlobStorageGroupInfo::TSubgroupVDisks& disks) const {
        TVector<ui32> counts(3, 0);
        for (ui32 realm = 0; realm < 3; ++realm) {
            for (ui32 domain = 0; domain < 3; ++domain) {
                counts[realm] += disks[realm + domain * 3];
            }
        }
        return Target == ETarget::WriteOk
            ? info.WritePolicy->CheckWriteOk(std::move(counts))
            : info.WritePolicy->CheckWriteDurability(std::move(counts));
    }

    void PreparePlacement(const TBlobState& state, const TBlobStorageGroupInfo& info, bool degraded,
            bool considerSlowAsError, TBlobStorageGroupType::TPartPlacement* placement, bool* fullPlacement) {
        ui32 preferred = Tactic == TEvBlobStorage::TEvPut::TacticMinLatency
            ? (EnableRequestMod3x3ForMinLatecy ? 3 : 2)
            : (degraded ? 2 : 1);
        for (; preferred <= 3; ++preferred) {
            placement->Records.clear();
            Prepare3dcPartPlacement(state, 3, 3, preferred, considerSlowAsError, Accelerate,
                *placement, *fullPlacement);
            TBlobStorageGroupInfo::TSubgroupVDisks planned(&info.GetTopology());
            for (ui32 diskIdx = 0; diskIdx < state.Disks.size(); ++diskIdx) {
                const auto& disk = state.Disks[diskIdx];
                const auto situation = disk.DiskParts[diskIdx % 3].Situation;
                if (situation == TBlobState::ESituation::Present
                        || situation == TBlobState::ESituation::Sent) {
                    planned |= {&info.GetTopology(), diskIdx};
                }
            }
            for (const auto& record : placement->Records) {
                planned |= {&info.GetTopology(), record.VDiskIdx};
            }
            if (CheckLayout(info, planned)) {
                return;
            }
        }
    }

public:
    TPut3dcPolicyStrategy(TEvBlobStorage::TEvPut::ETactic tactic, bool enableRequestMod3x3ForMinLatecy,
            ETarget target = ETarget::WriteOk, bool accelerate = false)
        : Tactic(tactic)
        , EnableRequestMod3x3ForMinLatecy(enableRequestMod3x3ForMinLatecy)
        , Target(target)
        , Accelerate(accelerate)
    {}

    EStrategyOutcome Process(TLogContext& logCtx, TBlobState& state, const TBlobStorageGroupInfo& info,
            TBlackboard& blackboard, TGroupDiskRequests& groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        if (info.Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc || !info.WritePolicy) {
            return EStrategyOutcome::Error("TPut3dcPolicyStrategy requires mirror-3-dc and a write policy");
        }
        if (!info.VDiskStates.empty()) {
            return EStrategyOutcome::Error("TPut3dcPolicyStrategy does not support non-default VDisk states yet");
        }

        if (Accelerate) {
            bool unresponsive = false;
            for (const auto& disk : state.Disks) {
                for (const auto& part : disk.DiskParts) {
                    unresponsive |= part.Situation == TBlobState::ESituation::Sent;
                }
            }
            if (!unresponsive) {
                return EStrategyOutcome::IN_PROGRESS;
            }
            blackboard.MarkSlowDisks(state, true, accelerationParams);
        }

        TBlobStorageGroupType::TPartPlacement placement;
        bool hasPlacement = false;
        const ui32 slowMask = Accelerate ? 0 : MakeSlowSubgroupDiskMask(state, blackboard, true, accelerationParams);
        for (bool considerSlowAsError : {true, false}) {
            TBlobStorageGroupInfo::TSubgroupVDisks success(&info.GetTopology());
            TBlobStorageGroupInfo::TSubgroupVDisks error(&info.GetTopology());
            bool degraded = false;
            Evaluate3dcSituation(state, 3, 3, info, Accelerate && considerSlowAsError, success, error, degraded);
            if (!Accelerate && considerSlowAsError) {
                if (!slowMask) {
                    continue;
                }
                const auto slow = TBlobStorageGroupInfo::TSubgroupVDisks::CreateFromMask(&info.GetTopology(), slowMask);
                if ((success | error) & slow) {
                    continue;
                }
                error += slow;
            }
            if (!info.GetQuorumChecker().CheckFailModelForSubgroup(error)) {
                if (Accelerate || considerSlowAsError) {
                    continue;
                }
                return EStrategyOutcome::Error("TPut3dcPolicyStrategy failed the Fail Model check");
            }
            if (CheckLayout(info, success)) {
                return EStrategyOutcome::DONE;
            }
            if (!CheckLayout(info, ~error)) {
                if (considerSlowAsError) {
                    continue;
                }
                return EStrategyOutcome::Error("Write placement policy cannot be satisfied");
            }
            bool fullPlacement;
            PreparePlacement(state, info, degraded, considerSlowAsError, &placement, &fullPlacement);
            if (considerSlowAsError && !fullPlacement) {
                continue;
            }
            hasPlacement = true;
            break;
        }
        if (hasPlacement && IsPutNeeded(state, placement)) {
            PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, placement);
        }
        return EStrategyOutcome::IN_PROGRESS;
    }
};

} // namespace NKikimr
