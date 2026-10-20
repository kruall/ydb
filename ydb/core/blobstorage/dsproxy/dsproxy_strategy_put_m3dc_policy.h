#pragma once

#include "dsproxy_strategy_base.h"

#include <algorithm>
#include <array>
#include <bit>
#include <optional>
#include <tuple>

namespace NKikimr {

// DSProxy completes placement according to WriteOk. Subsequent replication to
// WriteDurability belongs to VDisks and is independent of the client PUT.
class TPut3dcPolicyStrategy : public TStrategyBase {
private:
    const TEvBlobStorage::TEvPut::ETactic Tactic;
    const bool Accelerate;

    bool CheckLayout(const TBlobStorageGroupInfo& info,
            const TBlobStorageGroupInfo::TSubgroupVDisks& disks) const {
        TVector<ui32> counts(3, 0);
        for (ui32 realm = 0; realm < 3; ++realm) {
            for (ui32 domain = 0; domain < 3; ++domain) {
                counts[realm] += disks[realm + domain * 3];
            }
        }
        return info.WritePolicy->CheckWriteOk(std::move(counts));
    }

    using TCounts = std::array<ui32, 3>;

    struct TRealm {
        std::array<ui32, 4> PrefixMasks{};
        ui32 Available = 0;
        ui32 Present = 0;
        ui32 Errors = 0;
    };

    struct TCandidate {
        ui32 Mask = 0;
        ui32 Missing = 0;
        ui32 Errors = 0;
        ui32 Realms = 0;
        ui32 Parts = 0;
    };

    template<typename TCallback>
    static void ForEachAssignment(const TVector<ui32>& layout, const TCallback& callback) {
        if (layout.empty() || layout.size() > 3) {
            return;
        }
        TCounts counts{};
        for (ui32 i = 0; i < layout.size(); ++i) {
            if (!layout[i] || layout[i] > 3) {
                return;
            }
            counts[i] = layout[i];
        }
        std::sort(counts.begin(), counts.end());
        do {
            callback(counts);
        } while (std::next_permutation(counts.begin(), counts.end()));
    }

    std::array<TRealm, 3> GetRealms(const TBlobState& state, bool skipSlow) const {
        std::array<TRealm, 3> realms;
        for (ui32 realm = 0; realm < 3; ++realm) {
            auto& result = realms[realm];
            std::array<ui32, 3> disks{};
            for (ui32 domain = 0; domain < 3; ++domain) {
                const ui32 idx = realm + domain * 3;
                const auto& disk = state.Disks[idx];
                const auto situation = disk.DiskParts[realm].Situation;
                result.Errors += situation == TBlobState::ESituation::Error;
                result.Present += situation == TBlobState::ESituation::Present;
                if (situation != TBlobState::ESituation::Error
                        && (situation == TBlobState::ESituation::Present || !skipSlow || !disk.IsSlow)) {
                    disks[result.Available++] = idx;
                }
            }
            const auto rank = [&](ui32 idx) {
                const auto situation = state.Disks[idx].DiskParts[realm].Situation;
                if (situation == TBlobState::ESituation::Present) {
                    return 0;
                }
                // Sent does not reduce cost. Reuse it normally, hedge it when accelerating.
                return (situation == TBlobState::ESituation::Sent) == Accelerate ? 2 : 1;
            };
            std::sort(disks.begin(), disks.begin() + result.Available, [&](ui32 x, ui32 y) {
                return std::tuple(rank(x), x) < std::tuple(rank(y), y);
            });
            for (ui32 i = 0; i < result.Available; ++i) {
                result.PrefixMasks[i + 1] = result.PrefixMasks[i] | (1u << disks[i]);
            }
        }
        return realms;
    }

    static std::optional<TCandidate> MakeCandidate(const TCounts& counts, const std::array<TRealm, 3>& realms) {
        TCandidate result;
        for (ui32 realm = 0; realm < 3; ++realm) {
            const ui32 count = counts[realm];
            if (count > realms[realm].Available) {
                return std::nullopt;
            }
            result.Mask |= realms[realm].PrefixMasks[count];
            result.Missing += count > realms[realm].Present ? count - realms[realm].Present : 0;
            result.Errors += count ? realms[realm].Errors : 0;
            result.Realms += count != 0;
            result.Parts += count;
        }
        return result;
    }

    static bool BetterThroughput(const TCandidate& x, const TCandidate& y) {
        return std::tuple(3 - x.Realms, x.Parts, x.Missing, x.Errors, x.Mask)
            < std::tuple(3 - y.Realms, y.Parts, y.Missing, y.Errors, y.Mask);
    }

    static bool BetterLatency(const TCandidate& x, const TCandidate& y) {
        return std::tuple(x.Missing, x.Errors, 3 - x.Realms, x.Parts, x.Mask)
            < std::tuple(y.Missing, y.Errors, 3 - y.Realms, y.Parts, y.Mask);
    }

    bool PreparePlacement(const TBlobState& state, const TBlobStorageGroupInfo& info, bool skipSlow,
            TBlobStorageGroupType::TPartPlacement* placement) const {
        const auto realms = GetRealms(state, skipSlow);
        std::optional<TCandidate> best;
        TVector<const TVector<ui32>*> reachable;
        ui32 width = 0;
        ui32 height = 0;
        for (const auto& layout : info.WritePolicy->WriteOk) {
            bool found = false;
            ForEachAssignment(layout, [&](const TCounts& counts) {
                if (const auto candidate = MakeCandidate(counts, realms)) {
                    found = true;
                    if (!best || BetterThroughput(*candidate, *best)) {
                        best = candidate;
                    }
                }
            });
            if (found && Tactic == TEvBlobStorage::TEvPut::TacticMinLatency) {
                reachable.push_back(&layout);
                width = Max(width, static_cast<ui32>(layout.size()));
                height = Max(height, *std::max_element(layout.begin(), layout.end()));
            }
        }
        if (!best) {
            return false;
        }
        if (Tactic == TEvBlobStorage::TEvPut::TacticMinLatency) {
            const auto rectangle = [&](ui32 columns, ui32 rows) {
                std::optional<TCandidate> result;
                for (ui32 mask = 1; mask < 8; ++mask) {
                    if (static_cast<ui32>(std::popcount(mask)) != columns) {
                        continue;
                    }
                    TCounts counts{};
                    for (ui32 realm = 0; realm < 3; ++realm) {
                        if (mask & (1u << realm)) {
                            // Keep the reachable portion when errors leave holes in the rectangle.
                            counts[realm] = Min(rows, realms[realm].Available);
                        }
                    }
                    const auto candidate = MakeCandidate(counts, realms);
                    if (candidate->Realms != columns) {
                        continue;
                    }
                    auto sorted = counts;
                    std::sort(sorted.begin(), sorted.end(), std::greater<ui32>());
                    if (rows > height && sorted[0] <= height) {
                        continue; // No reachable position was added by the expansion.
                    }
                    bool coversAll = true;
                    for (const auto* layout : reachable) {
                        for (ui32 i = 0; i < layout->size(); ++i) {
                            coversAll &= sorted[i] >= (*layout)[i];
                        }
                    }
                    if (coversAll && (!result || BetterLatency(*candidate, *result))) {
                        result = candidate;
                    }
                }
                return result;
            };
            best = rectangle(width, height);
            const bool exact = std::any_of(reachable.begin(), reachable.end(), [&](const auto* layout) {
                return layout->size() == width
                    && std::all_of(layout->begin(), layout->end(), [&](ui32 count) { return count == height; });
            });
            if (exact) {
                std::optional<TCandidate> expanded;
                for (const auto& [columns, rows] : {std::pair(width + 1, height), std::pair(width, height + 1)}) {
                    if (columns <= 3 && rows <= 3) {
                        if (const auto candidate = rectangle(columns, rows);
                                candidate && (!expanded || BetterLatency(*candidate, *expanded))) {
                            expanded = candidate;
                        }
                    }
                }
                if (expanded) {
                    best = expanded;
                }
            }
        }
        if (!best) {
            return false;
        }
        placement->Records.clear();
        for (ui32 idx = 0; idx < 9; ++idx) {
            if (best->Mask & (1u << idx)) {
                placement->Records.emplace_back(idx, idx % 3);
            }
        }
        return true;
    }

public:
    TPut3dcPolicyStrategy(TEvBlobStorage::TEvPut::ETactic tactic, bool accelerate = false)
        : Tactic(tactic)
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

        TBlobStorageGroupInfo::TSubgroupVDisks success(&info.GetTopology());
        TBlobStorageGroupInfo::TSubgroupVDisks error(&info.GetTopology());
        bool degraded = false;
        Evaluate3dcSituation(state, 3, 3, info, false, success, error, degraded);
        if (!info.GetQuorumChecker().CheckFailModelForSubgroup(error)) {
            if (Accelerate) {
                return EStrategyOutcome::IN_PROGRESS;
            }
            return EStrategyOutcome::Error("TPut3dcPolicyStrategy failed the Fail Model check");
        }
        if (CheckLayout(info, success)) {
            return EStrategyOutcome::DONE;
        }
        if (!CheckLayout(info, ~error)) {
            if (Accelerate) {
                return EStrategyOutcome::IN_PROGRESS;
            }
            return EStrategyOutcome::Error("Write placement policy cannot be satisfied");
        }
        if (!Accelerate) {
            MakeSlowSubgroupDiskMask(state, blackboard, true, accelerationParams);
        }
        TBlobStorageGroupType::TPartPlacement placement;
        if ((PreparePlacement(state, info, true, &placement)
                || PreparePlacement(state, info, false, &placement)) && IsPutNeeded(state, placement)) {
            PreparePutsForPartPlacement(logCtx, state, info, groupDiskRequests, placement);
        }
        return EStrategyOutcome::IN_PROGRESS;
    }
};

} // namespace NKikimr
