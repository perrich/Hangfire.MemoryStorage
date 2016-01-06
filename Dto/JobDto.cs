using Hangfire.Storage.Monitoring;
using System;
using System.Collections.Generic;

namespace Hangfire.MemoryStorage.Dto
{
    public class JobDto : IExpirable, IIdentifiedData<string>
    {
        public JobDto()
        {
            History = new List<StateHistoryDto>();
            Parameters = new List<JobParameterDto>();
        }

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

        public IList<StateHistoryDto> History { get; set; }
        public IList<JobParameterDto> Parameters { get; set; }
    }
}