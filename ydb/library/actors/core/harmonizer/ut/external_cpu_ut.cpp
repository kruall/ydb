#include "../external_cpu.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(ExternalCpuTests) {

    Y_UNIT_TEST(ShouldRespectCpuMaskWhenParsingProcStat) {
        const TString procStat =
            "cpu  20 0 10 100 5 0 0 0 0 0\n"
            "cpu0 10 0 5 50 2 0 0 0 0 0\n"
            "cpu1 6 0 3 30 1 0 0 0 0 0\n"
            "cpu2 4 0 2 20 2 0 0 0 0 0\n";

        TCpuMask cpuMask;
        cpuMask.Set(0);
        cpuMask.Set(2);

        TExternalCpuCounters counters;
        UNIT_ASSERT(TryParseProcStat(procStat, cpuMask, counters));
        UNIT_ASSERT_VALUES_EQUAL(counters.CpuCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.BusyTicks, 21);
        UNIT_ASSERT_VALUES_EQUAL(counters.TotalTicks, 95);
    }

    Y_UNIT_TEST(ShouldParseProcessCpuTicksFromProcSelfStat) {
        const TString procSelfStat =
            "123 (sample process) R 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20";

        TExternalCpuCounters counters;
        UNIT_ASSERT(TryParseProcSelfStat(procSelfStat, counters));
        UNIT_ASSERT_VALUES_EQUAL(counters.ProcessTicks, 23);
    }

    Y_UNIT_TEST(ShouldCalculateExternalCpuLoad) {
        const TExternalCpuCounters previous{
            .BusyTicks = 100,
            .TotalTicks = 200,
            .ProcessTicks = 30,
            .CpuCount = 2,
        };
        const TExternalCpuCounters current{
            .BusyTicks = 160,
            .TotalTicks = 300,
            .ProcessTicks = 45,
            .CpuCount = 2,
        };

        const float externalCpu = CalculateExternalCpuLoad(previous, current);
        UNIT_ASSERT_DOUBLES_EQUAL(externalCpu, 0.9, 1e-6);
    }

    Y_UNIT_TEST(ShouldClampExternalCpuLoadToZeroWhenProcessConsumesEverything) {
        const TExternalCpuCounters previous{
            .BusyTicks = 100,
            .TotalTicks = 200,
            .ProcessTicks = 30,
            .CpuCount = 4,
        };
        const TExternalCpuCounters current{
            .BusyTicks = 140,
            .TotalTicks = 280,
            .ProcessTicks = 80,
            .CpuCount = 4,
        };

        const float externalCpu = CalculateExternalCpuLoad(previous, current);
        UNIT_ASSERT_DOUBLES_EQUAL(externalCpu, 0.0, 1e-6);
    }

} // Y_UNIT_TEST_SUITE(ExternalCpuTests)
