using System;

namespace Hangfire.MemoryStorage.Dto
{
    public class AggregatedCounterDto : IExpirable, IIntIdentifiedData
    {
        public string Key { get; set; }
        public long Value { get; set; }
        public DateTime? ExpireAt { get; set; }
        public int Id { get; set; }
    }
}