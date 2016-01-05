using System;

namespace Hangfire.MemoryStorage.Dto
{
    public class JobDto : IExpirable, IIdentifiedData<string>
    {
        public StateDto State { get; set; }

        public string StateName
        {
            get { return State != null ? State.Name : null; }
        }

        public string InvocationData { get; set; }
        public string Arguments { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime? ExpireAt { get; set; }
        public string Id { get; set; }
    }
}