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
            return new MemoryStorageConnection();
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new MemoryStorageMonitoringApi();
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            return new IServerComponent[]
            {
                new ExpirationManager(_options.JobExpirationCheckInterval),
                new CountersAggregator(_options.CountersAggregateInterval)
            };
        }
    }
}