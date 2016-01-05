using System;

namespace Hangfire.MemoryStorage.Dto
{
    public class StateDto : IIdentifiedData<int>
    {
        public string JobId { get; set; }
        public string Name { get; set; }
        public string Reason { get; set; }
        public DateTime CreatedAt { get; set; }
        public string Data { get; set; }
        public int Id { get; set; }
    }
}