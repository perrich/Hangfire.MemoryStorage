using System.Collections.Concurrent;

namespace Hangfire.MemoryStorage.UnitTest;

public class SimpleIntegrationTests
{
    private static object serverLock = new();
    private static BackgroundJobServer? server;
    private static ConcurrentDictionary<string, AutoResetEvent> resetEvents = new();

    public SimpleIntegrationTests()
    {
        GlobalConfiguration.Configuration
               .UseSimpleAssemblyNameTypeSerializer()
               .UseRecommendedSerializerSettings()
               .UseMemoryStorage();

        // only start one server process (shared across all Unittest) this is a limitation of HangFire
        // as the Jobs are submitted via a static method
        lock (serverLock)
        {
            if (server == null)
                server = new BackgroundJobServer();
        }
    }

    [Fact]
    public void RunSyncJob()
    {
        var resetEvent = resetEvents.GetOrAdd(nameof(RunSyncJob), new AutoResetEvent(false));
        
        var jobId = BackgroundJob.Enqueue(() => NotifyJobComplete(nameof(RunSyncJob)));

        var notified = resetEvent.WaitOne(TimeSpan.FromSeconds(30));
        Assert.True(notified, "Job did not run");
    }

    [Fact]
    public void RunAsyncJob()
    {
        var resetEvent = resetEvents.GetOrAdd(nameof(RunAsyncJob), new AutoResetEvent(false));

        var jobId = BackgroundJob.Enqueue(() => NotifyJobCompleteAsync(nameof(RunAsyncJob)));

        var notified = resetEvent.WaitOne(TimeSpan.FromSeconds(30));
        Assert.True(notified, "Job did not run");
    }

#pragma warning disable xUnit1013 // Needs to be public so HangFire can call ist
    public static void NotifyJobComplete(string key) => resetEvents[key].Set();

    public static async Task NotifyJobCompleteAsync(string key)
    {
        await Task.Run(() =>
        {
            resetEvents[key].Set();
        }).ConfigureAwait(false); // continue on another thread to check that the lock release works on another thread!
    }
#pragma warning restore xUnit1013
}