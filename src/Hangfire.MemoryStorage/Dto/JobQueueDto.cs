using System;

namespace Hangfire.MemoryStorage.Dto
{
    public class JobQueueDto : IIdentifiedData<int>
    {
        public string JobId { get; set; }
        public string Queue { get; set; }
        public DateTime AddedAt { get; set; }
        public DateTime? FetchedAt { get; set; }
        public int Id { get; set; }
    }
}