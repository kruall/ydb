#pragma once

#include "defs.h"
#include "keyvalue_events.h"
#include "keyvalue_intermediate.h"

#include <ydb/core/base/blobstorage.h>
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
    ui64 ChannelGeneration = 0;
    ui64 ChannelStep = 0;

    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    TReadIndexSnapshot Index;

    void Invalidate() const;
};

using TReadSharedSnapshotPtr = std::shared_ptr<const TReadSharedSnapshot>;
using TReadSharedSnapshotSubSystem = NActors::NTask::TServiceMapSubSystem<ui64, TReadSharedSnapshotPtr>;

struct TReadTaskArgs {
    ui64 TabletId = 0;
    NKikimrKeyValue::ReadRequest Request;
    TActorId KeyValueActorId;
    TActorId RespondTo;
    NWilson::TTraceId TraceId;
    bool UsePayloadInResponse = false;
};

struct TReadTaskResult {
    std::unique_ptr<TEvKeyValue::TEvReadResponse> Response;
    bool NeedsFallback = true;
};

NActors::NTask::task<TReadTaskResult> RunReadTask(TReadTaskArgs args);

} // namespace NKikimr::NKeyValue::NTask
