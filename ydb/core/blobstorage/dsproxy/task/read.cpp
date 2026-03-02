#include "read.h"

namespace NKikimr::NBlobStorage::NDSProxy::NTask {

    NActors::NTask::task<TReadTaskResult> RunReadTask(TReadTaskArgs args) {
        Y_UNUSED(args);
        co_return nullptr;
    }

} // namespace NKikimr::NBlobStorage::NDSProxy::NTask
