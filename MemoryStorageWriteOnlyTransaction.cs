using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.MemoryStorage.Utilities;
using Hangfire.Storage;
using IHangfireState = Hangfire.States.IState;

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
                Data.Create<StateDto>(new StateDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof (StateDto)),
                    JobId = jobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = DateTime.UtcNow,
                    Data = JobHelper.ToJson(state.SerializeData())
                });
            });
        }

        public void AddToQueue(string queue, string jobId)
        {
            QueueCommand(() =>
            {
                Data.Create<JobQueueDto>(new JobQueueDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof (JobQueueDto)),
                    Queue = queue,
                    AddedAt = DateTime.UtcNow,
                    JobId = jobId
                });
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
                    set = Data.Create<SetDto>(new SetDto
                    {
                        Id = AutoIncrementIdGenerator.GenerateId(typeof (SetDto)),
                        Key = key,
                        Value = value
                    });
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
                Data.Create<ListDto>(new ListDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof (ListDto)),
                    Key = key,
                    Value = value
                });
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
                    Data.Delete(typeof (ListDto), list);
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
                    Data.Delete(typeof (SetDto), set);
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
                    Data.Delete(typeof (HashDto), hash);
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

                var stateData = Data.Create<StateDto>(new StateDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof (StateDto)),
                    JobId = jobId,
                    Name = state.Name,
                    Reason = state.Reason,
                    CreatedAt = DateTime.UtcNow,
                    Data = JobHelper.ToJson(state.SerializeData())
                });

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
                        hash = Data.Create<HashDto>(new HashDto
                        {
                            Id = AutoIncrementIdGenerator.GenerateId(typeof (HashDto)),
                            Key = key,
                            Field = local.Key
                        });
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