using System.Collections.Generic;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.MemoryStorage
{
    public class MemoryStorage : JobStorage
    {
        private readonly MemoryStorageOptions _options;

        public MemoryStorage() : this(new MemoryStorageOptions())
        {
        }

        public MemoryStorage(MemoryStorageOptions options)
        {
            _options = options;
        }

        public override IStorageConnection GetConnection()
        {
            return new MemoryStorageConnection(_options.FetchNextJobTimeout);
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new MemoryStorageMonitoringApi();
        }

#pragma warning disable 618
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new ExpirationManager(_options.JobExpirationCheckInterval);
            yield return new CountersAggregator(_options.CountersAggregateInterval);
        }
    }
}