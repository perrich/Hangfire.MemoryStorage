using Hangfire.MemoryStorage.Database;

namespace Hangfire.MemoryStorage
{
    public static class GlobalConfigurationExtensions
    {
        public static IGlobalConfiguration<MemoryStorage> UseMemoryStorage(this IGlobalConfiguration configuration)
        {
            var storageOptions = new MemoryStorageOptions();

            return configuration.UseMemoryStorage(storageOptions);
        }

        public static IGlobalConfiguration<MemoryStorage> UseMemoryStorage(this IGlobalConfiguration configuration,
            MemoryStorageOptions storageOptions)
        {
            var storage = new MemoryStorage(storageOptions);

            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<MemoryStorage> UseMemoryStorage(this IGlobalConfiguration configuration,
            MemoryStorageOptions storageOptions, Data data)
        {
            var storage = new MemoryStorage(storageOptions, data);

            return configuration.UseStorage(storage);
        }
    }
}