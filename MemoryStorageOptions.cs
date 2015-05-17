using System;

namespace Hangfire.MemoryStorage
{
    public class MemoryStorageOptions
    {
        public MemoryStorageOptions()
        {
            JobExpirationCheckInterval = TimeSpan.FromHours(1);
            CountersAggregateInterval = TimeSpan.FromMinutes(5);
        }

        public TimeSpan JobExpirationCheckInterval { get; set; }
        public TimeSpan CountersAggregateInterval { get; set; }
    }
}