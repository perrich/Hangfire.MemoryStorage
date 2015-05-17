using System;

namespace Hangfire.MemoryStorage.Dto
{
    public class ListDto : IExpirable, IKeyValued, IIntIdentifiedData
    {
        public DateTime? ExpireAt { get; set; }
        public int Id { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
    }
}