
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.MemoryStorage;

namespace Hangfire.MemoryStorage.UnitTest;

public class MemoryStorageMonitoringApiTests
{
    private Data data;
    private MemoryStorageMonitoringApi monitoringApi;

    public MemoryStorageMonitoringApiTests()
    {
        data = new Data();
        monitoringApi = new MemoryStorageMonitoringApi(data);
    }

    [Fact]
    public void MonitoringShouldReturnSumOfCounterIfTwoAggregatedCounterDtoHaveSameKey()
    {
        var key = string.Format("stats:succeeded:{0}", DateTime.UtcNow.AddHours(-2).ToString("yyyy-MM-dd-HH"));
        data.Create(new AggregatedCounterDto { Id = 1, Key = key, Value = 5 });
        data.Create(new AggregatedCounterDto { Id = 2, Key = key, Value = 2 }); // Creation in CountersAggregator can produce two object in concurrent environment

        var stats = monitoringApi.HourlySucceededJobs().ToArray();
        Assert.Equal(7, stats[2].Value);
    }
}