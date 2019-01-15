using System.Collections.Generic;
using Hangfire.MemoryStorage.Database;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.MemoryStorage
{
    public class MemoryStorage : JobStorage
    {
        private readonly MemoryStorageOptions _options;
        private readonly Data _data;

        public MemoryStorage() : this(new MemoryStorageOptions(), new Data())
        {
        }

        public MemoryStorage(MemoryStorageOptions options) : this(options, new Data())
        {
        }

        public MemoryStorage(MemoryStorageOptions options, Data data)
        {
            _options = options;
            _data = data;
        }

        public override IStorageConnection GetConnection()
        {
            return new MemoryStorageConnection(_data, _options.FetchNextJobTimeout);
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new MemoryStorageMonitoringApi(_data);
        }

#pragma warning disable 618
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new ExpirationManager(_data, _options.JobExpirationCheckInterval);
            yield return new CountersAggregator(_data, _options.CountersAggregateInterval);
        }
    }
}