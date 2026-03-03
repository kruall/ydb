#pragma once

#include "defs.h"
#include "keyvalue_intermediate.h"
#include "keyvalue_key_range.h"

#include <ydb/core/base/logoblob.h>
#include <ydb/core/protos/msgbus_kv.pb.h>
#include <ydb/library/actors/task/service_map_subsystem.h>
#include <ydb/library/actors/task/task.h>

#include <atomic>
#include <memory>

namespace NKikimr::NKeyValue::NTask {

struct TReadChainItemSnapshot {
    TLogoBlobID LogoBlobId;
    TRope InlineData;
    ui64 Offset = 0;

    bool IsInline() const {
        return !LogoBlobId.IsValid();
    }

    ui64 GetSize() const {
        return IsInline() ? InlineData.size() : LogoBlobId.BlobSize();
    }

    friend bool operator<(ui64 left, const TReadChainItemSnapshot& right) {
        return left < right.Offset;
    }
};

struct TReadIndexRecordSnapshot {
    TVector<TReadChainItemSnapshot> Chain;
    ui64 CreationUnixTime = 0;

    ui64 GetFullValueSize() const;
    ui32 GetReadItems(ui64 offset, ui64 size, TIntermediate::TRead& read) const;
};

using TReadIndexSnapshot = TMap<TString, TReadIndexRecordSnapshot>;

struct TReadSharedSnapshot {
    mutable std::atomic<bool> IsActual = false;
    ui64 Epoch = 0;
    ui64 TabletId = 0;
    ui64 UserGeneration = 0;

    // Same values that are currently written into TIntermediate constructor.
    ui64 ChannelGeneration = 0;
    ui64 ChannelStep = 0;

    TReadIndexSnapshot Index;

    void Invalidate() const;
};

using TReadSharedSnapshotPtr = std::shared_ptr<const TReadSharedSnapshot>;
using TReadSharedSnapshotSubSystem = NActors::NTask::TServiceMapSubSystem<ui64, TReadSharedSnapshotPtr>;

struct TPrepareReadTaskCommonArgs {
    TReadSharedSnapshotPtr Snapshot;
    TActorId KeyValueActorId;
    TActorId RespondTo;
    NWilson::TTraceId TraceId;
    bool UsePayloadInResponse = false;
};

struct TPrepareReadTaskArgs {
    TPrepareReadTaskCommonArgs Common;
    NKikimrKeyValue::ReadRequest Request;
};

struct TPrepareReadRangeTaskArgs {
    TPrepareReadTaskCommonArgs Common;
    NKikimrKeyValue::ReadRangeRequest Request;
};

struct TPrepareReadTaskResult {
    THolder<TIntermediate> Intermediate;
    TRequestType::EType RequestType = TRequestType::ReadOnly;
    bool NeedsFallback = true;
};

NActors::NTask::task<TPrepareReadTaskResult> RunPrepareReadTask(TPrepareReadTaskArgs args);
NActors::NTask::task<TPrepareReadTaskResult> RunPrepareReadRangeTask(TPrepareReadRangeTaskArgs args);

} // namespace NKikimr::NKeyValue::NTask
