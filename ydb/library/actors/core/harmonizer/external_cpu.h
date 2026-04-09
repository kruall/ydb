#pragma once

#include "defs.h"

#include <ydb/library/actors/util/cpumask.h>

#include <util/generic/string.h>

#include <optional>

namespace NActors {

    struct TExternalCpuCounters {
        ui64 BusyTicks = 0;
        ui64 TotalTicks = 0;
        ui64 ProcessTicks = 0;
        ui32 CpuCount = 0;

        TString ToString() const;
    };

    bool TryReadExternalCpuCounters(const TCpuMask& cpuMask, TExternalCpuCounters& counters);
    bool TryParseProcStat(TStringBuf stat, const TCpuMask& cpuMask, TExternalCpuCounters& counters);
    bool TryParseProcSelfStat(TStringBuf stat, TExternalCpuCounters& counters);
    float CalculateExternalCpuLoad(const TExternalCpuCounters& previous, const TExternalCpuCounters& current);

    class TExternalCpuLoadCollector {
    public:
        float Pull();

    private:
        std::optional<TExternalCpuCounters> Previous;
    };

} // namespace NActors
