using System;
using System.Linq;
using System.Threading;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.Server;

namespace Hangfire.MemoryStorage
{
#pragma warning disable 618
    public class CountersAggregator : IServerComponent
#pragma warning restore 618
    {
        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private readonly TimeSpan _aggregateInterval;

        public CountersAggregator(TimeSpan aggregateInterval)
        {
            _aggregateInterval = aggregateInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var removedCount = 0;

            do
            {
                var counters = Data.GetEnumeration<CounterDto>().Take(NumberOfRecordsInSinglePass).ToList();

                var groupedCounters = counters.GroupBy(c => c.Key).Select(g => new
                {
                    g.Key,
                    Value = g.Sum(c => c.Value),
                    ExpireAt = g.Max(c => c.ExpireAt)
                });

                foreach (var counter in groupedCounters)
                {
                    var aggregate = Data.GetEnumeration<AggregatedCounterDto>()
                        .FirstOrDefault(a => a.Key == counter.Key);

                    if (aggregate == null)
                    {
                        aggregate = new AggregatedCounterDto
                        {
                            Id = AutoIncrementIdGenerator.GenerateId(typeof(AggregatedCounterDto)),
                            Key = counter.Key,
                            Value = 0,
                            ExpireAt = DateTime.MinValue
                        };

                        Data.Create(aggregate);
                    }

                    aggregate.Value += counter.Value;

                    if (counter.ExpireAt > aggregate.ExpireAt)
                    {
                        aggregate.ExpireAt = counter.ExpireAt;
                    }
                }

                removedCount = counters.Count;

                Data.Delete(counters);

                if (removedCount > 0)
                {
                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount != 0);

            cancellationToken.WaitHandle.WaitOne(_aggregateInterval);
        }

        public override string ToString()
        {
            return "Counter Table Aggregator";
        }
    }
}