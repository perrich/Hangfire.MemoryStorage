using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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

        private readonly AutoResetEvent CommitTriggerEvent;

        private readonly Data _data;

        public MemoryStorageWriteOnlyTransaction(Data data, AutoResetEvent commitTriggerEvent)
        {
            this.CommitTriggerEvent = commitTriggerEvent;
            _data = data;
        }

        public override void AddJobState(string jobId, IHangfireState state)
        {
            QueueCommand(() =>
            {
                var job = _data.Get<JobDto>(jobId);
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

                _data.Create(jobQueue);
            });
        }

        public override void AddToSet(string key, string value)
        {
            this.AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            QueueCommand(() =>
            {
                var set = _data.GetEnumeration<SetDto>().SingleOrDefault(s => s.Key == key && s.Value == value);
                if (set == null)
                {
                    set = new SetDto
                    {
                        Id = AutoIncrementIdGenerator.GenerateId(typeof(SetDto)),
                        Key = key,
                        Value = value
                    };

                    _data.Create(set);
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

            this.CommitTriggerEvent.Set();
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var job = _data.Get<JobDto>(jobId);
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
                var setitems = _data.GetEnumeration<SetDto>().Where(s => s.Key == key);
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
                var hash = _data.GetEnumeration<HashDto>().Where(s => s.Key == key);
                foreach (var hashitem in hash)
                {
                    hashitem.ExpireAt = DateTime.UtcNow.Add(expireIn);
                }
            });
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var list = _data.GetEnumeration<ListDto>().Where(s => s.Key == key);
                foreach (var listitem in list)
                {
                    listitem.ExpireAt = DateTime.UtcNow.Add(expireIn);
                }
            });
        }

        public override void IncrementCounter(string key)
        {
            QueueCommand(() => { CounterUtilities.IncrementCounter(_data, key, false); });
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var counter = CounterUtilities.IncrementCounter(_data, key, false);
                counter.ExpireAt = DateTime.UtcNow.Add(expireIn);
            });
        }

        public override void DecrementCounter(string key)
        {
            QueueCommand(() => { CounterUtilities.IncrementCounter(_data, key, true); });
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(() =>
            {
                var counter = CounterUtilities.IncrementCounter(_data, key, true);
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

                _data.Create(list);
            });
        }

        public override void PersistJob(string jobId)
        {
            QueueCommand(() =>
            {
                var job = _data.Get<JobDto>(jobId);
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
                var list = _data.GetEnumeration<ListDto>().SingleOrDefault(j => j.Key == key && j.Value == value);
                if (list != null)
                {
                    _data.Delete(list);
                }
            });
        }

        public override void RemoveFromSet(string key, string value)
        {
            QueueCommand(() =>
            {
                var set = _data.GetEnumeration<SetDto>().SingleOrDefault(j => j.Key == key && j.Value == value);
                if (set != null)
                {
                    _data.Delete(set);
                }
            });
        }

        public override void RemoveHash(string key)
        {
            Guard.ArgumentNotNull(key, "key");

            QueueCommand(() =>
            {
                var hash = _data.GetEnumeration<HashDto>().Where(j => j.Key == key).ToList();
                if (hash.Any())
                {
                    _data.Delete(hash);
                }
            });
        }

        public override void RemoveSet(string key)
        {
            Guard.ArgumentNotNull(key, "key");

            QueueCommand(() =>
            {
                var set = _data.GetEnumeration<SetDto>().Where(j => j.Key == key).ToList();
                if (set.Any())
                {
                    _data.Delete(set);
                }
            });
        }

        public override void SetJobState(string jobId, IHangfireState state)
        {
            QueueCommand(() =>
            {
                var job = _data.Get<JobDto>(jobId);
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
                    var hash = _data.GetEnumeration<HashDto>().SingleOrDefault(h => h.Key == key && h.Field == local.Key);
                    if (hash == null)
                    {
                        hash = new HashDto
                        {
                            Id = AutoIncrementIdGenerator.GenerateId(typeof(HashDto)),
                            Key = key,
                            Field = local.Key
                        };

                        _data.Create(hash);
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
            QueueCommand(() =>
            {
                var set = _data.GetEnumeration<SetDto>().Where(s => s.Key == key);
                foreach (var item in set)
                {
                    item.ExpireAt = null;
                }
            });
        }

        public override void PersistHash(string key)
        {
            QueueCommand(() =>
            {
                var hash = _data.GetEnumeration<HashDto>().Where(s => s.Key == key);
                foreach (var item in hash)
                {
                    item.ExpireAt = null;
                }
            });
        }

        public override void PersistList(string key)
        {
            QueueCommand(() =>
            {
                var list = _data.GetEnumeration<ListDto>().Where(s => s.Key == key);
                foreach (var item in list)
                {
                    item.ExpireAt = null;
                }
            });
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            this.QueueCommand(() =>
            {

                var existingSet = _data.GetEnumeration<SetDto>().Where(s => s.Key == key).Select(s => s.Value).ToList();
                foreach (var item in items.Where(i => !existingSet.Contains(i)))
                {
                    var newSet = new SetDto
                    {
                        Id = AutoIncrementIdGenerator.GenerateId(typeof(SetDto)),
                        Key = key,
                        Value = item
                    };
                    _data.Create(newSet);
                }
            });
        }
    }
}