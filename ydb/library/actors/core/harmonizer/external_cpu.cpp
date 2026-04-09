#include "external_cpu.h"

#include <ydb/library/actors/util/affinity.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NActors {

namespace {

    constexpr const char* ProcStatPath = "/proc/stat";
    constexpr const char* ProcSelfStatPath = "/proc/self/stat";

    bool TryParseUi64(TStringBuf value, ui64& result) {
        return TryFromString<ui64>(value, result);
    }

} // namespace

TString TExternalCpuCounters::ToString() const {
    return TStringBuilder()
        << "{BusyTicks: " << BusyTicks
        << ", TotalTicks: " << TotalTicks
        << ", ProcessTicks: " << ProcessTicks
        << ", CpuCount: " << CpuCount
        << '}';
}

bool TryParseProcStat(TStringBuf stat, const TCpuMask& cpuMask, TExternalCpuCounters& counters) {
#if defined(_linux_)
    counters.BusyTicks = 0;
    counters.TotalTicks = 0;
    counters.CpuCount = 0;

    for (TStringBuf line : StringSplitter(stat).Split('\n')) {
        if (!line.StartsWith("cpu") || line.size() <= 3 || !IsAsciiDigit(line[3])) {
            continue;
        }

        TStringBuf cpuSuffix = line.SubStr(3);
        TStringBuf cpuIdToken = cpuSuffix.Before(' ');
        ui32 cpuId = 0;
        if (!TryFromString(cpuIdToken, cpuId) || !cpuMask.IsSet(cpuId)) {
            continue;
        }

        TVector<TStringBuf> values;
        StringSplitter(line.After(' ')).Split(' ').SkipEmpty().Collect(&values);
        if (values.size() < 5) {
            return false;
        }

        ui64 fields[8] = {};
        for (size_t idx = 0; idx < Min<size_t>(values.size(), Y_ARRAY_SIZE(fields)); ++idx) {
            if (!TryParseUi64(values[idx], fields[idx])) {
                return false;
            }
        }

        const ui64 idle = fields[3];
        const ui64 iowait = fields[4];
        ui64 total = 0;
        for (size_t idx = 0; idx < Min<size_t>(values.size(), Y_ARRAY_SIZE(fields)); ++idx) {
            total += fields[idx];
        }

        counters.BusyTicks += total - idle - iowait;
        counters.TotalTicks += total;
        ++counters.CpuCount;
    }

    return counters.CpuCount > 0;
#else
    Y_UNUSED(stat);
    Y_UNUSED(cpuMask);
    Y_UNUSED(counters);
    return false;
#endif
}

bool TryParseProcSelfStat(TStringBuf stat, TExternalCpuCounters& counters) {
#if defined(_linux_)
    TStringBuf tail = stat.RAfter(')').After(' ');
    TVector<TStringBuf> values;
    StringSplitter(tail).Split(' ').SkipEmpty().Collect(&values);
    if (values.size() <= 12) {
        return false;
    }

    ui64 utime = 0;
    ui64 stime = 0;
    if (!TryParseUi64(values[11], utime) || !TryParseUi64(values[12], stime)) {
        return false;
    }

    counters.ProcessTicks = utime + stime;
    return true;
#else
    Y_UNUSED(stat);
    Y_UNUSED(counters);
    return false;
#endif
}

bool TryReadExternalCpuCounters(const TCpuMask& cpuMask, TExternalCpuCounters& counters) {
#if defined(_linux_)
    TUnbufferedFileInput procStatFile(ProcStatPath);
    TString procStat = procStatFile.ReadAll();
    if (!TryParseProcStat(procStat, cpuMask, counters)) {
        return false;
    }

    TUnbufferedFileInput procSelfStatFile(ProcSelfStatPath);
    TString procSelfStat = procSelfStatFile.ReadAll();
    return TryParseProcSelfStat(procSelfStat, counters);
#else
    Y_UNUSED(cpuMask);
    Y_UNUSED(counters);
    return false;
#endif
}

float CalculateExternalCpuLoad(const TExternalCpuCounters& previous, const TExternalCpuCounters& current) {
    if (current.CpuCount == 0 || previous.CpuCount != current.CpuCount) {
        return 0.0f;
    }
    if (current.BusyTicks < previous.BusyTicks || current.TotalTicks <= previous.TotalTicks || current.ProcessTicks < previous.ProcessTicks) {
        return 0.0f;
    }

    const ui64 busyDelta = current.BusyTicks - previous.BusyTicks;
    const ui64 totalDelta = current.TotalTicks - previous.TotalTicks;
    const ui64 processDelta = current.ProcessTicks - previous.ProcessTicks;
    if (totalDelta == 0) {
        return 0.0f;
    }

    const double externalBusyTicks = static_cast<double>(busyDelta - Min(busyDelta, processDelta));
    const double cpuCount = static_cast<double>(current.CpuCount);
    return static_cast<float>(Min(cpuCount, cpuCount * externalBusyTicks / static_cast<double>(totalDelta)));
}

float TExternalCpuLoadCollector::Pull() {
#if defined(_linux_)
    TAffinity affinity;
    affinity.Current();
    TCpuMask cpuMask = affinity;

    TExternalCpuCounters current;
    if (!TryReadExternalCpuCounters(cpuMask, current)) {
        Previous.reset();
        return 0.0f;
    }

    float externalCpu = 0.0f;
    if (Previous) {
        externalCpu = CalculateExternalCpuLoad(*Previous, current);
    }
    Previous = current;
    return externalCpu;
#else
    return 0.0f;
#endif
}

} // namespace NActors
