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
    }
}