#include "stat_processor.h"

namespace NKikimr::NBsController {

    class TStatProcessorActor : public TActorBootstrapped<TStatProcessorActor> {
        using TGroupCleanupSchedule = TMultiMap<TInstant, std::pair<TGroupId, TActorId>>;

        struct TPerGroupRecord {
            struct TPerAggregatorInfo {
                TGroupStat Stat;
                TGroupCleanupSchedule::iterator ScheduleIter;
            };

            TGroupStat Accum;
            THashMap<TActorId, TPerAggregatorInfo> PerAggregatorInfo;
            TEvControllerNotifyGroupChange::TVDiskServiceIds VDiskServiceIds;
            TGroupLatencyStats Stats;

            bool Update(const NKikimrBlobStorage::TEvGroupStatReport& report, TInstant now, TGroupId groupId,
                    TGroupCleanupSchedule& schedule) {
                const TActorId vdiskServiceId = ActorIdFromProto(report.GetVDiskServiceId());
                if (!VDiskServiceIds.contains(vdiskServiceId)) {
                    return false;
                }

                TGroupStat stat;
                if (stat.Deserialize(report)) {
                    auto& item = PerAggregatorInfo[vdiskServiceId];
                    Accum.Replace(stat, item.Stat);
                    item.Stat = std::move(stat);

                    const TInstant barrier = now + TDuration::Seconds(30);
                    if (item.ScheduleIter != TGroupCleanupSchedule::iterator()) {
                        schedule.erase(item.ScheduleIter);
                    }
                    item.ScheduleIter = schedule.emplace(barrier, std::make_pair(groupId, vdiskServiceId));
                    return true;
                }
                return false;
            }

            bool Cleanup(const TActorId& vdiskServiceId, TGroupCleanupSchedule::iterator scheduleIter) {
                auto it = PerAggregatorInfo.find(vdiskServiceId);
                if (it == PerAggregatorInfo.end() || it->second.ScheduleIter != scheduleIter) {
                    return false;
                }
                const auto& item = it->second;
                Accum.Subtract(item.Stat);
                PerAggregatorInfo.erase(it);
                return true;
            }

            bool UpdateVDiskServiceIds(TEvControllerNotifyGroupChange::TVDiskServiceIds&& vdiskServiceIds,
                    TGroupCleanupSchedule& schedule) {
                VDiskServiceIds = std::move(vdiskServiceIds);

                bool changed = false;
                for (auto it = PerAggregatorInfo.begin(); it != PerAggregatorInfo.end(); ) {
                    if (VDiskServiceIds.contains(it->first)) {
                        ++it;
                    } else {
                        const auto next = std::next(it);
                        Accum.Subtract(it->second.Stat);
                        schedule.erase(it->second.ScheduleIter);
                        PerAggregatorInfo.erase(it);
                        it = next;
                        changed = true;
                    }
                }
                return changed;
            }

            void Clear(TGroupCleanupSchedule& schedule) {
                for (const auto& [vdiskServiceId, item] : PerAggregatorInfo) {
                    Y_UNUSED(vdiskServiceId);
                    schedule.erase(item.ScheduleIter);
                }
            }

            void RecalculatePercentiles() {
                Stats.PutTabletLog = Accum.GetPercentile(TGroupStat::EKind::PUT_TABLET_LOG, 0.90);
                Stats.PutUserData = Accum.GetPercentile(TGroupStat::EKind::PUT_USER_DATA, 0.90);
                Stats.GetFast = Accum.GetPercentile(TGroupStat::EKind::GET_FAST, 0.90);
            }
        };

        static constexpr TDuration UpdatePeriod = TDuration::Seconds(10);

        TActorId ParentActorId;
        TMap<TGroupId, TPerGroupRecord> Groups;
        TSet<TGroupId> UpdatedGroupIds;
        TGroupCleanupSchedule GroupCleanupSchedule;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BSC_STAT_PROCESSOR;
        }

        void Bootstrap(const TActorId& parentActorId) {
            ParentActorId = parentActorId;
            Become(&TThis::StateFunc, UpdatePeriod, new TEvents::TEvWakeup);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvBlobStorage::TEvControllerUpdateGroupStat, Handle);
            hFunc(TEvControllerNotifyGroupChange, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )

        void Handle(TEvBlobStorage::TEvControllerUpdateGroupStat::TPtr& ev) {
            TSet<TGroupId> ids;
            const TInstant now = TActivationContext::Now();

            // apply new report
            auto& record = ev->Get()->Record;
            for (const NKikimrBlobStorage::TEvGroupStatReport& item : record.GetPerGroupReport()) {
                const TGroupId groupId = TGroupId::FromProto(&item, &NKikimrBlobStorage::TEvGroupStatReport::GetGroupId);
                auto it = Groups.find(groupId);
                if (it != Groups.end()) {
                    if (it->second.Update(item, now, groupId, GroupCleanupSchedule)) {
                        ids.insert(groupId);
                    }
                }
            }

            CleanupExpired(now, ids);

            // recalculate percentiles for changed groups
            RecalculateGroups(ids);
        }

        void Handle(TEvControllerNotifyGroupChange::TPtr& ev) {
            for (TGroupId groupId : ev->Get()->Deleted) {
                if (auto it = Groups.find(groupId); it != Groups.end()) {
                    it->second.Clear(GroupCleanupSchedule);
                    Groups.erase(it);
                }
                UpdatedGroupIds.erase(groupId);
            }
            for (auto& [groupId, vdiskServiceIds] : ev->Get()->Updated) {
                auto [it, inserted] = Groups.try_emplace(groupId);
                if (it->second.UpdateVDiskServiceIds(std::move(vdiskServiceIds), GroupCleanupSchedule) || inserted) {
                    it->second.RecalculatePercentiles();
                    UpdatedGroupIds.insert(groupId);
                }
            }
        }

        void HandleWakeup() {
            TSet<TGroupId> ids;
            CleanupExpired(TActivationContext::Now(), ids);
            RecalculateGroups(ids);

            if (UpdatedGroupIds) {
                auto ev = MakeHolder<TEvControllerCommitGroupLatencies>();

                const bool scanLowerBound = UpdatedGroupIds.size() <= Groups.size() / 10;
                auto it = Groups.begin();
                for (TGroupId groupId : UpdatedGroupIds) {
                    if (scanLowerBound) {
                        // use lower bound logic as the number of updated groups is not so big
                        it = Groups.lower_bound(groupId);
                    } else {
                        // advance second iterator to the corresponding group -- it must exist
                        for (; it != Groups.end() && it->first < groupId; ++it)
                        {}
                    }
                    Y_ABORT_UNLESS(it != Groups.end() && it->first == groupId);

                    // report this group to the event
                    ev->Updates.emplace_hint(ev->Updates.end(), groupId, it->second.Stats);
                }

                // report updates to the controller
                Send(ParentActorId, ev.Release());

                // reset updates
                UpdatedGroupIds.clear();
            }

            Schedule(UpdatePeriod, new TEvents::TEvWakeup);
        }

        void CleanupExpired(TInstant now, TSet<TGroupId>& ids) {
            while (GroupCleanupSchedule && GroupCleanupSchedule.begin()->first <= now) {
                const auto scheduleIt = GroupCleanupSchedule.begin();
                const auto [groupId, vdiskServiceId] = scheduleIt->second;
                if (auto groupIt = Groups.find(groupId); groupIt != Groups.end() &&
                        groupIt->second.Cleanup(vdiskServiceId, scheduleIt)) {
                    ids.insert(groupId);
                }
                GroupCleanupSchedule.erase(scheduleIt);
            }
        }

        void RecalculateGroups(const TSet<TGroupId>& ids) {
            for (const TGroupId& groupId : ids) {
                if (auto it = Groups.find(groupId); it != Groups.end()) {
                    it->second.RecalculatePercentiles();
                    UpdatedGroupIds.insert(groupId);
                }
            }
        }
    };

    IActor *CreateStatProcessorActor() {
        return new TStatProcessorActor;
    }

} // NKikimr::NBsController
