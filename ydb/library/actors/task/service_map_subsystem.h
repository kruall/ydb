#pragma once

#include <util/generic/algorithm.h>
#include <ydb/library/actors/core/servicemap.h>
#include <ydb/library/actors/core/subsystem.h>
#include <ydb/library/actors/util/ticket_lock.h>

namespace NActors::NTask {

    template<class TKey, class TValue, class THashFunc = THash<TKey>>
    class TServiceMapSubSystem final : public ISubSystem {
    public:
        TValue Find(const TKey& key) {
            return Storage_.Find(key);
        }

        TValue Update(const TKey& key, const TValue& value) {
            TTicketLock::TGuard guard(&WriteLock_);
            return Storage_.Update(key, value);
        }

        bool Erase(const TKey& key) {
            TTicketLock::TGuard guard(&WriteLock_);
            return Storage_.Erase(key);
        }

    private:
        NActors::TServiceMap<TKey, TValue, THashFunc> Storage_;
        TTicketLock WriteLock_;
    };

} // namespace NActors::NTask
