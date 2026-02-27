#pragma once

// Backward-compat header kept for easier grepping and local rebases.
// Prefer including task_system.h directly.

#include "task_system.h"

namespace NActors::NTask {
    using TMpmcCoroutineQueue = TTaskSystem;
} // namespace NActors::NTask

