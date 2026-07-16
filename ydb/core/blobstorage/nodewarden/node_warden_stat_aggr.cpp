#include "node_warden_impl.h"

using namespace NKikimr;
using namespace NStorage;

void TNodeWarden::ReportLatencies() {
    std::unique_ptr<TEvBlobStorage::TEvControllerUpdateGroupStat> ev;

    for (const auto& kv : PerAggregatorInfo) {
        const TAggregatorInfo& info = kv.second;
        const TGroupID groupId(info.GroupId);
        if (!ev) {
            ev.reset(new TEvBlobStorage::TEvControllerUpdateGroupStat);
        }
        auto& record = ev->Record;
        auto *pb = record.AddPerGroupReport();
        pb->SetGroupId(info.GroupId);
        ActorIdToProto(kv.first, pb->MutableVDiskServiceId());
        info.Stat.Serialize(pb);
    }

    if (ev) {
        SendToController(std::move(ev));
    }
}

void TNodeWarden::Handle(TEvGroupStatReport::TPtr ev) {
    TEvGroupStatReport *msg = ev->Get();
    TActorId vdiskServiceId = msg->GetVDiskServiceId();

    TGroupStat stat;
    const auto it = RunningAggregators.find(vdiskServiceId);
    if (it != RunningAggregators.end() && it->second.ActorId == ev->Sender &&
            it->second.GroupId == msg->GetGroupId() && msg->GetStat(stat)) {
        PerAggregatorInfo[vdiskServiceId] = TAggregatorInfo{
            msg->GetGroupId(),
            std::move(stat)
        };
    }
}

void TNodeWarden::StartAggregator(const TActorId& vdiskServiceId, ui32 groupId) {
    if (!RunningAggregators.contains(vdiskServiceId)) {
        const TActorId groupStatAggregatorId = MakeGroupStatAggregatorId(vdiskServiceId);
        const TActorId actorId = Register(CreateGroupStatAggregatorActor(groupId, vdiskServiceId),
            TMailboxType::Revolving, AppData()->SystemPoolId);
        TActivationContext::ActorSystem()->RegisterLocalService(groupStatAggregatorId, actorId);
        const auto [_, inserted] = RunningAggregators.emplace(vdiskServiceId,
            TRunningAggregatorInfo{actorId, groupId});
        Y_ABORT_UNLESS(inserted);
    }
}

void TNodeWarden::StopAggregator(const TActorId& vdiskServiceId) {
    if (auto node = RunningAggregators.extract(vdiskServiceId)) {
        TActivationContext::Send(new IEventHandle(
            TEvents::TSystem::Poison, 0, node.mapped().ActorId, {}, nullptr, 0));
        PerAggregatorInfo.erase(vdiskServiceId);
    }
}
