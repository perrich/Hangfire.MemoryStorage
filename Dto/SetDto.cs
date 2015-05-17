using System;

namespace Hangfire.MemoryStorage.Dto
{
    public class SetDto : IExpirable, IKeyValued, IIntIdentifiedData
    {
        public long Score { get; set; }
        public DateTime? ExpireAt { get; set; }
        public int Id { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
    }
}