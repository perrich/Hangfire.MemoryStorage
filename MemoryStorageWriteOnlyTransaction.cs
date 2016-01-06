using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.MemoryStorage.Utilities;
using Hangfire.Storage;
using IHangfireState = Hangfire.States.IState;
using Hangfire.Storage.Monitoring;

namespace Hangfire.MemoryStorage
{
    /// <summary>
    ///     Fake transaction in memory (command are delayed and done when needed)
    /// </summary>
    public class MemoryStorageWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private IList<Action> commandsList = new List<Action>();

        public void AddJobState(string jobId, IHangfireState state)
        {
            QueueCommand(() =>
            {
                var job = Data.Get<JobDto>(jobId);
                if (job == null)
                {
                    return;
                }

                DateTime createdAt = DateTime.UtcNow;
                Dictionary<string, string> serializedStateData = state.SerializeData();

                var stateData = new StateDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof(StateDto)),
                    JobId = jobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = createdAt,
                    Data = JobHelper.ToJson(serializedStateData)
                };
                
                var stateHistory = new StateHistoryDto
                {
                    StateName = state.Name,
                    CreatedAt = createdAt,
                    Reason = state.Reason,
                    Data = serializedStateData
                };

                job.History.Add(stateHistory);
            });
        }

        public void AddToQueue(string queue, string jobId)
        {
            QueueCommand(() =>
            {
                var jobQueue = new JobQueueDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof (JobQueueDto)),
                    Queue = queue,
                    AddedAt = DateTime.UtcNow,
                    JobId = jobId
                };

                Data.Create(jobQueue);
            });
        }

        public void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public void AddToSet(string key, string value, double score)
        {
            QueueCommand(() =>
            {
                var set = Data.GetEnumeration<SetDto>().SingleOrDefault(s => s.Key == key && s.Value == value);
                if (set == null)
                {
                    set = new SetDto
                    {
                        Id = AutoIncrementIdGenerator.GenerateId(typeof (SetDto)),
                        Key = key,
                        Value = value
                    };

                    Data.Create(set);
                }

                set.Score = (long) score;
            });
        }

        public void Commit()
        {
            var commands = commandsList;
            commandsList = new List<Action>();

            foreach (var cmd in commands)
            {
                cmd();
            }
        }

        public void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var job = Data.Get<JobDto>(jobId);
                if (job != null)
                {
                    job.ExpireAt = DateTime.UtcNow.Add(expireIn);
                }
            });
        }

        public void IncrementCounter(string key)
        {
            QueueCommand(() => { CounterUtilities.IncrementCounter(key, false); });
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var counter = CounterUtilities.IncrementCounter(key, false);
                counter.ExpireAt = DateTime.UtcNow.Add(expireIn);
            });
        }

        public void DecrementCounter(string key)
        {
            QueueCommand(() => { CounterUtilities.IncrementCounter(key, true); });
        }

        public void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var counter = CounterUtilities.IncrementCounter(key, true);
                counter.ExpireAt = DateTime.UtcNow.Add(expireIn);
            });
        }

        public void InsertToList(string key, string value)
        {
            QueueCommand(() =>
            {
                var list = new ListDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof (ListDto)),
                    Key = key,
                    Value = value
                };

                Data.Create(list);
            });
        }

        public void PersistJob(string jobId)
        {
            QueueCommand(() =>
            {
                var job = Data.Get<JobDto>(jobId);
                if (job != null)
                {
                    job.ExpireAt = null;
                }
            });
        }

        public void RemoveFromList(string key, string value)
        {
            QueueCommand(() =>
            {
                var list = Data.GetEnumeration<ListDto>().SingleOrDefault(j => j.Key == key && j.Value == value);
                if (list != null)
                {
                    Data.Delete(list);
                }
            });
        }

        public void RemoveFromSet(string key, string value)
        {
            QueueCommand(() =>
            {
                var set = Data.GetEnumeration<SetDto>().SingleOrDefault(j => j.Key == key && j.Value == value);
                if (set != null)
                {
                    Data.Delete(set);
                }
            });
        }

        public void RemoveHash(string key)
        {
            Guard.ArgumentNotNull(key, "key");

            QueueCommand(() =>
            {
                var hash = Data.GetEnumeration<HashDto>().Where(j => j.Key == key).ToList();
                if (hash.Any())
                {
                    Data.Delete(hash);
                }
            });
        }

        public void SetJobState(string jobId, IHangfireState state)
        {
            QueueCommand(() =>
            {
                var job = Data.Get<JobDto>(jobId);
                if (job == null)
                {
                    return;
                }

                DateTime createdAt = DateTime.UtcNow;
                Dictionary<string, string> serializedStateData = state.SerializeData();

                var stateData = new StateDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof(StateDto)),
                    JobId = jobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = createdAt,
                    Data = JobHelper.ToJson(serializedStateData)
                };

                var stateHistory = new StateHistoryDto
                {
                    StateName = state.Name,
                    CreatedAt = createdAt,
                    Reason = state.Reason,
                    Data = serializedStateData
                };

                job.History.Add(stateHistory);

                job.State = stateData;
            });
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            Guard.ArgumentNotNull(key, "key");
            Guard.ArgumentNotNull(keyValuePairs, "keyValuePairs");

            foreach (var kvp in keyValuePairs)
            {
                var local = kvp;
                QueueCommand(() =>
                {
                    var hash = Data.GetEnumeration<HashDto>().SingleOrDefault(h => h.Key == key && h.Field == local.Key);
                    if (hash == null)
                    {
                        hash = new HashDto
                        {
                            Id = AutoIncrementIdGenerator.GenerateId(typeof (HashDto)),
                            Key = key,
                            Field = local.Key
                        };

                        Data.Create(hash);
                    }

                    hash.Value = local.Value;
                });
            }
        }

        public void Dispose()
        {
        }

        public void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
        }

        private void QueueCommand(Action command)
        {
            commandsList.Add(command);
        }
    }
}