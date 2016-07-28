using System;

namespace Hangfire.MemoryStorage.Dto
{
    public class HashDto : IExpirable, IKeyValued, IIdentifiedData<int>
    {
        public string Field { get; set; }
        public DateTime? ExpireAt { get; set; }
        public int Id { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
    }
}