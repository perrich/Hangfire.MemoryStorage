using Hangfire.MemoryStorage.Database;

namespace Hangfire.MemoryStorage
{
    public static class GlobalConfigurationExtensions
    {
        public static MemoryStorage UseMemoryStorage(this IGlobalConfiguration configuration)
        {
            var storageOptions = new MemoryStorageOptions();

            return configuration.UseMemoryStorage(storageOptions);
        }

        public static MemoryStorage UseMemoryStorage(this IGlobalConfiguration configuration,
            MemoryStorageOptions storageOptions)
        {
            var storage = new MemoryStorage(storageOptions);

            configuration.UseStorage(storage);

            return storage;
        }

        public static MemoryStorage UseMemoryStorage(this IGlobalConfiguration configuration,
            MemoryStorageOptions storageOptions, Data data)
        {
            var storage = new MemoryStorage(storageOptions, data);

            configuration.UseStorage(storage);

            return storage;
        }
    }
}