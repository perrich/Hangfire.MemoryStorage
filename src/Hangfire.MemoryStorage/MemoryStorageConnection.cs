using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.MemoryStorage.Utilities;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.MemoryStorage
{
    public class MemoryStorageConnection : JobStorageConnection
    {
        private static readonly object FetchJobsLock = new object();
        private readonly TimeSpan _fetchNextJobTimeout;
        private readonly Data _data;

        public MemoryStorageConnection(Data data, TimeSpan fetchNextJobTimeout)
        {
            _fetchNextJobTimeout = fetchNextJobTimeout;
            _data = data;
        }

        internal static readonly AutoResetEvent NewItemInQueueEvent = new AutoResetEvent(true);

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return LocalLock.AcquireLock(resource, timeout);
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            Guard.ArgumentNotNull(serverId, "serverId");
            Guard.ArgumentNotNull(context, "context");

            var server = _data.GetOrCreate(serverId, new ServerDto
            {
                Id = serverId
            });

            var data = new
            {
                context.WorkerCount,
                context.Queues,
                StartedAt = DateTime.UtcNow
            };

            server.LastHeartbeat = DateTime.UtcNow;
            server.Data = JobHelper.ToJson(data);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt,
            TimeSpan expireIn)
        {
            Guard.ArgumentNotNull(job, "job");
            Guard.ArgumentNotNull(parameters, "parameters");

            var invocationData = InvocationData.Serialize(job);

            var jobData = new JobDto
            {
                Id = Guid.NewGuid().ToString(),
                InvocationData = JobHelper.ToJson(invocationData),
                Arguments = invocationData.Arguments,
                CreatedAt = createdAt,
                ExpireAt = createdAt.Add(expireIn)
            };

            _data.Create(jobData);

            if (parameters.Count > 0)
            {
                var list = parameters.Select(kvp => new JobParameterDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof (JobParameterDto)),
                    JobId = jobData.Id,
                    Name = kvp.Key,
                    Value = kvp.Value
                }).ToList();

                jobData.Parameters = list;
            }

            return jobData.Id;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new MemoryStorageWriteOnlyTransaction(_data, NewItemInQueueEvent);
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            Guard.ArgumentNotNull(queues, "queues");
            Guard.ArgumentCondition(queues.Length == 0, "queues", "Queues cannot be empty");

            JobQueueDto queue;

            while (true)
            {
                var timeout = DateTime.UtcNow.Add(_fetchNextJobTimeout.Negate());

                lock (FetchJobsLock)
                {
                    var jobQueues = _data.GetEnumeration<JobQueueDto>();

                    queue = (from q in jobQueues
                        where queues.Contains(q.Queue)
                              && (!q.FetchedAt.HasValue || q.FetchedAt.Value < timeout)
                        orderby q.AddedAt descending
                        select q).FirstOrDefault();

                    if (queue != null)
                    {
                        queue.FetchedAt = DateTime.UtcNow;
                        break;
                    }
                }

                WaitHandle.WaitAny(new[] { cancellationToken.WaitHandle, NewItemInQueueEvent }, TimeSpan.FromSeconds(15));
                cancellationToken.ThrowIfCancellationRequested();
            }

            return new MemoryStorageFetchedJob(_data, queue);
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            Guard.ArgumentNotNull(key, "key");

            var hashes = _data.GetEnumeration<HashDto>()
                .Where(h => h.Key == key)
                .ToDictionary(h => h.Field, h => h.Value);

            return hashes.Count == 0 ? null : hashes;
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            Guard.ArgumentNotNull(key, "key");

            var values = _data.GetEnumeration<SetDto>().Where(s => s.Key == key).Select(s => s.Value);

            return new HashSet<string>(values);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            Guard.ArgumentNotNull(key, "key");

            if (toScore < fromScore)
            {
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            }

            var set = _data.GetEnumeration<SetDto>()
                .Where(s => s.Key == key && s.Score >= fromScore && s.Score <= toScore)
                .OrderByDescending(s => s.Score)
                .FirstOrDefault();

            return set == null ? null : set.Value;
        }

        public override JobData GetJobData(string jobId)
        {
            Guard.ArgumentNotNull(jobId, "jobId");

            var jobData = _data.Get<JobDto>(jobId);
            if (jobData == null)
            {
                return null;
            }

            var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);

            invocationData.Arguments = jobData.Arguments;

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = jobData.StateName,
                CreatedAt = jobData.CreatedAt,
                LoadException = loadException
            };
        }

        public override string GetJobParameter(string id, string name)
        {
            Guard.ArgumentNotNull(id, "id");
            Guard.ArgumentNotNull(name, "name");

            var jobData = _data.Get<JobDto>(id);
            if (jobData == null)
            {
                return null;
            }

            var parameter = jobData.Parameters.Where(p => p.Name == name).FirstOrDefault();
            
            return parameter == null ? null : parameter.Value;
        }

        public override long GetListCount(string key)
        {
            return GetCount<ListDto>(key);
        }

        public override long GetSetCount(string key)
        {
            return GetCount<SetDto>(key);
        }

        public override long GetHashCount(string key)
        {
            return GetCount<HashDto>(key);
        }

        public override StateData GetStateData(string jobId)
        {
            Guard.ArgumentNotNull(jobId, "jobId");

            var jobData = _data.Get<JobDto>(jobId);
            if (jobData == null || jobData.State == null)
            {
                return null;
            }

            return new StateData
            {
                Name = jobData.State.Name,
                Reason = jobData.State.Reason,
                Data = new Dictionary<string, string>(
                    JobHelper.FromJson<Dictionary<string, string>>(jobData.State.Data),
                    StringComparer.OrdinalIgnoreCase)
            };
        }

        public override string GetValueFromHash(string key, string name)
        {
            Guard.ArgumentNotNull(key, "key");
            Guard.ArgumentNotNull(name, "name");

            return _data.GetEnumeration<HashDto>()
                .Where(h => h.Key == key && h.Field == name)
                .Select(h => h.Value).SingleOrDefault();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            Guard.ArgumentNotNull(key, "key");

            return _data.GetEnumeration<ListDto>()
                .Where(l => l.Key == key)
                .OrderBy(l => l.Id)
                .Select(l => l.Value)
                .ToList();
        }

        public override long GetCounter(string key)
        {
            Guard.ArgumentNotNull(key, "key");

            return CounterUtilities.GetCombinedCounter(_data, key);
        }

        public override TimeSpan GetHashTtl(string key)
        {
            return GetExpireAt<HashDto>(key);
        }

        public override TimeSpan GetListTtl(string key)
        {
            return GetExpireAt<ListDto>(key);
        }

        public override TimeSpan GetSetTtl(string key)
        {
            return GetExpireAt<SetDto>(key);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            Guard.ArgumentNotNull(key, "key");

            var count = (endingAt - startingFrom) + 1;

            return _data.GetEnumeration<ListDto>()
                .Where(l => l.Key == key)
                .OrderBy(l => l.Id)
                .Skip(startingFrom)
                .Take(count)
                .Select(l => l.Value)
                .ToList();
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            Guard.ArgumentNotNull(key, "key");

            var count = (endingAt - startingFrom) + 1;

            return _data.GetEnumeration<SetDto>()
                .Where(s => s.Key == key)
                .OrderBy(s => s.Id)
                .Skip(startingFrom)
                .Take(count)
                .Select(s => s.Value)
                .ToList();
        }

        public override void Heartbeat(string serverId)
        {
            Guard.ArgumentNotNull(serverId, "serverId");

            var server = _data.Get<ServerDto>(serverId);
            if (server == null)
            {
                return;
            }

            server.LastHeartbeat = DateTime.UtcNow;
        }

        public override void RemoveServer(string serverId)
        {
            Guard.ArgumentNotNull(serverId, "serverId");

            var server = _data.Get<ServerDto>(serverId);
            if (server != null)
            {
                _data.Delete(server);
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", "timeOut");
            }

            var timeOutAt = DateTime.UtcNow.Add(timeOut.Negate());
            var servers = _data.GetEnumeration<ServerDto>().Where(s => s.LastHeartbeat < timeOutAt).ToList();

            _data.Delete(servers);

            return servers.Count;
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            Guard.ArgumentNotNull(id, "id");
            Guard.ArgumentNotNull(name, "name");

            var jobData = _data.Get<JobDto>(id);
            if (jobData == null)
            {
                return;
            }

            var parameter = jobData.Parameters.Where(p => p.Name == name).FirstOrDefault();

            if (parameter == null)
            {
                parameter = new JobParameterDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof (JobParameterDto)),
                    JobId = id,
                    Name = name
                };

                jobData.Parameters.Add(parameter);
            }

            parameter.Value = value;
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            Guard.ArgumentNotNull(key, "key");
            Guard.ArgumentNotNull(keyValuePairs, "keyValuePairs");

            foreach (var kvp in keyValuePairs)
            {
                var hash = _data.GetEnumeration<HashDto>().SingleOrDefault(h => h.Key == key && h.Field == kvp.Key);
                if (hash == null)
                {
                    hash = new HashDto
                    {
                        Id = AutoIncrementIdGenerator.GenerateId(typeof (HashDto)),
                        Key = key,
                        Field = kvp.Key
                    };

                    _data.Create(hash);
                }

                hash.Value = kvp.Value;
            }
        }

        private TimeSpan GetExpireAt<T>(string key) where T : IExpirable
        {
            Guard.ArgumentNotNull(key, "key");

            var date = _data.GetEnumeration<T>().Select(l => l.ExpireAt).Min();

            return date.HasValue ? date.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        private long GetCount<T>(string key) where T : IKeyValued
        {
            Guard.ArgumentNotNull(key, "key");

            return _data.GetEnumeration<T>().Count(h => h.Key == key);
        }
    }
}
