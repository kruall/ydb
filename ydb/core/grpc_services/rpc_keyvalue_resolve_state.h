#pragma once

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/task/service_map_subsystem.h>

#include <util/generic/hash.h>

namespace NKikimr::NGRpcService {

    struct TKeyValueResolveKey {
        TString Database;
        TString Path;

        bool operator==(const TKeyValueResolveKey&) const = default;
    };

    struct TKeyValueResolveKeyHash {
        size_t operator()(const TKeyValueResolveKey& key) const noexcept {
            size_t h = THash<TString>()(key.Database);
            h = h * 1664525u + THash<TString>()(key.Path);
            return h;
        }
    };

    struct TKeyValueResolveValue {
        TString CanonizedPath;
        THashMap<ui64, ui64> PartitionToTabletId;
        TIntrusivePtr<TSecurityObject> SecurityObject;
        TMonotonic ExpireAt = TMonotonic::Zero();
    };

    using TKeyValueResolveValuePtr = std::shared_ptr<const TKeyValueResolveValue>;
    using TKeyValueResolveSubSystem =
        NActors::NTask::TServiceMapSubSystem<TKeyValueResolveKey, TKeyValueResolveValuePtr, TKeyValueResolveKeyHash>;

} // namespace NKikimr::NGRpcService
