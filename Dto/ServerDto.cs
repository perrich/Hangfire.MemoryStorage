using System;

namespace Hangfire.MemoryStorage.Dto
{
    public class ServerDto : IStringIdentifiedData
    {
        public string Data { get; set; }
        public DateTime LastHeartbeat { get; set; }
        public string Id { get; set; }
    }
}