namespace Hangfire.MemoryStorage.Entities
{
    public class EnqueuedAndFetchedCountDto
    {
        public int EnqueuedCount { get; set; }
        public int FetchedCount { get; set; }
    }
}