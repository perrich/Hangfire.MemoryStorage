using System;

namespace Hangfire.MemoryStorage.Dto
{
    public class ServerDto : IIdentifiedData<string>
    {
        public string Data { get; set; }
        public DateTime LastHeartbeat { get; set; }
        public string Id { get; set; }
    }
}