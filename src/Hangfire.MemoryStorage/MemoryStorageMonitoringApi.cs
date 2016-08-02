using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.MemoryStorage.Entities;
using Hangfire.MemoryStorage.Utilities;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using ServerDto = Hangfire.Storage.Monitoring.ServerDto;

namespace Hangfire.MemoryStorage
{
    public class MemoryStorageMonitoringApi : IMonitoringApi
    {
        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobs(
                from,
                count,
                DeletedState.StateName,
                (jsonJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                });
        }

        public long DeletedListCount()
        {
            return GetNumberOfJobsByStateName(DeletedState.StateName);
        }

        public long EnqueuedCount(string queue)
        {
            return Data.GetEnumeration<JobQueueDto>().Count(q => q.Queue == queue && !q.FetchedAt.HasValue);
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var enqueuedJobIds = QueueApi.GetEnqueuedJobIds(queue, from, perPage, false);

            return EnqueuedJobs(enqueuedJobIds);
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return GetTimelineStats("failed");
        }

        public long FailedCount()
        {
            return GetNumberOfJobsByStateName(FailedState.StateName);
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobs(from, count,
                FailedState.StateName,
                (jsonJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = jsonJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                });
        }

        public long FetchedCount(string queue)
        {
            return Data.GetEnumeration<JobQueueDto>().Count(q => q.Queue == queue && q.FetchedAt.HasValue);
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var fetchedJobIds = QueueApi.GetEnqueuedJobIds(queue, from, perPage, true);

            return FetchedJobs(fetchedJobIds);
        }

        public StatisticsDto GetStatistics()
        {
            var states =
                Data.GetEnumeration<JobDto>()
                    .Where(j => j.StateName != null)
                    .GroupBy(j => j.StateName)
                    .ToDictionary(j => j.Key, j => j.Count());

            Func<string, int> getCountIfExists = name => states.ContainsKey(name) ? states[name] : 0;

            var succeded = CounterUtilities.GetCombinedCounter("stats:succeeded");
            var deleted = CounterUtilities.GetCombinedCounter("stats:deleted");

            var recurringJobs = Data.GetEnumeration<SetDto>().Count(c => c.Key == "recurring-jobs");
            var servers = Data.GetEnumeration<Dto.ServerDto>().Count();

            var stats = new StatisticsDto
            {
                Enqueued = getCountIfExists(EnqueuedState.StateName),
                Failed = getCountIfExists(FailedState.StateName),
                Processing = getCountIfExists(ProcessingState.StateName),
                Scheduled = getCountIfExists(ScheduledState.StateName),
                Servers = servers,
                Succeeded = succeded,
                Deleted = deleted,
                Recurring = recurringJobs
            };

            return stats;
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return GetHourlyTimelineStats("failed");
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return GetHourlyTimelineStats("succeeded");
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            Guard.ArgumentNotNull(jobId, "jobId");

            var job = Data.Get<JobDto>(jobId);
            if (job == null)
            {
                return null;
            }

            var jobParameters = job.Parameters.ToDictionary(p => p.Name, p => p.Value);

            return new JobDetailsDto
            {
                CreatedAt = job.CreatedAt,
                ExpireAt = job.ExpireAt,
                Job = DeserializeJob(job.InvocationData, job.Arguments),
                History = job.History,
                Properties = jobParameters
            };
        }

        public long ProcessingCount()
        {
            return GetNumberOfJobsByStateName(ProcessingState.StateName);
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobs(from, count,
                ProcessingState.StateName,
                (jsonJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"])
                });
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var queuesData = Data.GetEnumeration<JobQueueDto>().ToList();
            var queues = queuesData.GroupBy(q => q.Queue).ToDictionary(q => q.Key, q => q.Count());

            var query =
                from kvp in queues
                let enqueuedJobIds = QueueApi.GetEnqueuedJobIds(kvp.Key, 0, 5, false)
                let counters = QueueApi.GetEnqueuedAndFetchedCount(queuesData, kvp.Key)
                select new QueueWithTopEnqueuedJobsDto
                {
                    Name = kvp.Key,
                    Length = counters.EnqueuedCount,
                    Fetched = counters.FetchedCount,
                    FirstJobs = EnqueuedJobs(enqueuedJobIds)
                };

            return query.ToList();
        }

        public long ScheduledCount()
        {
            return GetNumberOfJobsByStateName(ScheduledState.StateName);
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobs(from, count,
                ScheduledState.StateName,
                (jsonJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                });
        }

        public IList<ServerDto> Servers()
        {
            var servers = Data.GetEnumeration<Dto.ServerDto>();

            var query =
                from server in servers
                let serverData = JobHelper.FromJson<ServerData>(server.Data)
                select new ServerDto
                {
                    Name = server.Id,
                    Heartbeat = server.LastHeartbeat,
                    Queues = serverData.Queues,
                    StartedAt = serverData.StartedAt ?? DateTime.MinValue,
                    WorkersCount = serverData.WorkerCount
                };

            return query.ToList();
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return GetTimelineStats("succeeded");
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobs(from, count,
                SucceededState.StateName,
                (jsonJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?) long.Parse(stateData["PerformanceDuration"]) +
                          (long?) long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                });
        }

        public long SucceededListCount()
        {
            return GetNumberOfJobsByStateName(SucceededState.StateName);
        }

        private static Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();

            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            return GetTimelineStats(dates, x => string.Format("stats:{0}:{1}", type, x.ToString("yyyy-MM-dd-HH")));
        }

        public Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var dates = new List<DateTime>();

            for (var i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            return GetTimelineStats(dates, x => string.Format("stats:{0}:{1}", type, x.ToString("yyyy-MM-dd")));
        }

        private static Dictionary<DateTime, long> GetTimelineStats(List<DateTime> dates,
            Func<DateTime, string> formatorAction)
        {
            var counters = Data.GetEnumeration<AggregatedCounterDto>();
            var keyMap = dates.ToDictionary(formatorAction, x => x);

            var valuesMap = (from c in counters
                where keyMap.Keys.Contains(c.Key)
                select c).ToDictionary(o => o.Key, o => o.Value);

            foreach (var key in keyMap.Keys.Where(key => !valuesMap.ContainsKey(key)))
            {
                valuesMap.Add(key, 0);
            }

            return keyMap.ToDictionary(k => k.Value, k => valuesMap[k.Key]);
        }

        private static JobList<FetchedJobDto> FetchedJobs(IEnumerable<string> jobIds)
        {
            var jobs = Data.GetEnumeration<JobDto>();
            var queues = Data.GetEnumeration<JobQueueDto>();

            var query = (from j in jobs
                join q in queues on j.Id equals q.JobId
                where jobIds.Contains(j.Id) && j.State != null && q.FetchedAt.HasValue
                select new JsonJob
                {
                    Arguments = j.Arguments,
                    CreatedAt = j.CreatedAt,
                    ExpireAt = j.ExpireAt,
                    Id = j.Id,
                    InvocationData = j.InvocationData,
                    StateReason = j.State.Reason,
                    StateData = j.State.Data,
                    StateName = j.StateName
                })
                .AsEnumerable()
                .Select(job => new KeyValuePair<string, FetchedJobDto>(job.Id, new FetchedJobDto
                {
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
                    State = job.StateName,
                    FetchedAt = job.FetchedAt
                }));

            return new JobList<FetchedJobDto>(query);
        }

        private static JobList<EnqueuedJobDto> EnqueuedJobs(IEnumerable<string> jobIds)
        {
            var jobs = Data.GetEnumeration<JobDto>();
            var queues = Data.GetEnumeration<JobQueueDto>();

            var query = (from j in jobs
                join q in queues on j.Id equals q.JobId
                where jobIds.Contains(j.Id) && j.State != null && !q.FetchedAt.HasValue
                select new JsonJob
                {
                    Arguments = j.Arguments,
                    CreatedAt = j.CreatedAt,
                    ExpireAt = j.ExpireAt,
                    Id = j.Id,
                    InvocationData = j.InvocationData,
                    StateReason = j.State.Reason,
                    StateData = j.State.Data,
                    StateName = j.StateName
                }).ToList();

            return DeserializeJobs(query, (jsonJob, job, stateData) => new EnqueuedJobDto
            {
                Job = job,
                State = jsonJob.StateName,
                EnqueuedAt = jsonJob.StateName == EnqueuedState.StateName
                    ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                    : null
            });
        }

        private static JobList<TDto> GetJobs<TDto>(
            int from,
            int count,
            string stateName,
            Func<JsonJob, Job, Dictionary<string, string>, TDto> selector)
        {
            var jobs =
                Data.GetEnumeration<JobDto>().Where(j => j.StateName == stateName).OrderByDescending(j => j.CreatedAt);

            var query = (from job in jobs
                select new JsonJob
                {
                    Id = job.Id,
                    InvocationData = job.InvocationData,
                    Arguments = job.Arguments,
                    CreatedAt = job.CreatedAt,
                    ExpireAt = job.ExpireAt,
                    StateReason = job.State.Reason,
                    StateData = job.State.Data
                }
                ).Skip(from).Take(count).ToList();

            return DeserializeJobs(query, selector);
        }

        private static Job DeserializeJob(string invocationData, string arguments)
        {
            var data = JobHelper.FromJson<InvocationData>(invocationData);
            data.Arguments = arguments;

            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        private static JobList<TDto> DeserializeJobs<TDto>(
            IEnumerable<JsonJob> jobs,
            Func<JsonJob, Job, Dictionary<string, string>, TDto> selector)
        {
            var result = from job in jobs
                let deserializedData = JobHelper.FromJson<Dictionary<string, string>>(job.StateData)
                let stateData = deserializedData != null
                    ? new Dictionary<string, string>(deserializedData, StringComparer.OrdinalIgnoreCase)
                    : null
                let dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData)
                select new KeyValuePair<string, TDto>(job.Id, dto);

            return new JobList<TDto>(result);
        }

        private static long GetNumberOfJobsByStateName(string stateName)
        {
            var count = Data.GetEnumeration<JobDto>().Count(j => j.StateName == stateName);

            return count;
        }
    }
}