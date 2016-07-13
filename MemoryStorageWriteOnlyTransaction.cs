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
    public class MemoryStorageWriteOnlyTransaction : JobStorageTransaction, IWriteOnlyTransaction
    {
        private IList<Action> commandsList = new List<Action>();

        public override void AddJobState(string jobId, IHangfireState state)
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

        public override void AddToQueue(string queue, string jobId)
        {
            QueueCommand(() =>
            {
                var jobQueue = new JobQueueDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof(JobQueueDto)),
                    Queue = queue,
                    AddedAt = DateTime.UtcNow,
                    JobId = jobId
                };

                Data.Create(jobQueue);
            });
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            QueueCommand(() =>
            {
                var set = Data.GetEnumeration<SetDto>().SingleOrDefault(s => s.Key == key && s.Value == value);
                if (set == null)
                {
                    set = new SetDto
                    {
                        Id = AutoIncrementIdGenerator.GenerateId(typeof(SetDto)),
                        Key = key,
                        Value = value
                    };

                    Data.Create(set);
                }

                set.Score = (long) score;
            });
        }

        public override void Commit()
        {
            var commands = commandsList;
            commandsList = new List<Action>();

            foreach (var cmd in commands)
            {
                cmd();
            }
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
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

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var setitems = Data.GetEnumeration<SetDto>().Where(s => s.Key == key);
                foreach (var setitem in setitems)
                {
                    setitem.ExpireAt = DateTime.UtcNow.Add(expireIn);
                }
            });
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var hash = Data.GetEnumeration<HashDto>().Where(s => s.Key == key);
                foreach (var hashitem in hash)
                {
                    hashitem.ExpireAt = DateTime.UtcNow.Add(expireIn);
                }
            });
        }

        public override void IncrementCounter(string key)
        {
            QueueCommand(() => { CounterUtilities.IncrementCounter(key, false); });
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var counter = CounterUtilities.IncrementCounter(key, false);
                counter.ExpireAt = DateTime.UtcNow.Add(expireIn);
            });
        }

        public override void DecrementCounter(string key)
        {
            QueueCommand(() => { CounterUtilities.IncrementCounter(key, true); });
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var counter = CounterUtilities.IncrementCounter(key, true);
                counter.ExpireAt = DateTime.UtcNow.Add(expireIn);
            });
        }

        public override void InsertToList(string key, string value)
        {
            QueueCommand(() =>
            {
                var list = new ListDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof(ListDto)),
                    Key = key,
                    Value = value
                };

                Data.Create(list);
            });
        }

        public override void PersistJob(string jobId)
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

        public override void RemoveFromList(string key, string value)
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

        public override void RemoveFromSet(string key, string value)
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

        public override void RemoveHash(string key)
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

        public override void RemoveSet(string key)
        {
            Guard.ArgumentNotNull(key, "key");

            QueueCommand(() =>
            {
                var set = Data.GetEnumeration<SetDto>().Where(j => j.Key == key).ToList();
                if (set.Any())
                {
                    Data.Delete(set);
                }
            });
        }

        public override void SetJobState(string jobId, IHangfireState state)
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

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
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
                            Id = AutoIncrementIdGenerator.GenerateId(typeof(HashDto)),
                            Key = key,
                            Field = local.Key
                        };

                        Data.Create(hash);
                    }

                    hash.Value = local.Value;
                });
            }
        }

        public override void Dispose()
        {
            base.Dispose();
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
        }

        private void QueueCommand(Action command)
        {
            commandsList.Add(command);
        }

        public override void PersistSet(string key)
        {
            // noop
        }

        public override void PersistHash(string key)
        {
            // noop
        }

        public override void PersistList(string key)
        {
            // noop
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            this.QueueCommand(() =>
            {
                var set = new SetDto();
                Data.Create(set);
            });
        }
    }
}