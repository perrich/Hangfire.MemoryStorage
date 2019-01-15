using System;
using System.Linq;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;

namespace Hangfire.MemoryStorage.Utilities
{
    public static class CounterUtilities
    {
        public static long GetCombinedCounter(Data data, string key)
        {
            var counters =
                data.GetEnumeration<CounterDto>().Where(c => c.Key == key).Select(c => c.Value).DefaultIfEmpty(0).Sum();
            var aggregatedCounters =
                data.GetEnumeration<AggregatedCounterDto>()
                    .Where(c => c.Key == key)
                    .Select(c => c.Value)
                    .DefaultIfEmpty(0)
                    .Sum();

            return counters + aggregatedCounters;
        }

        public static CounterDto IncrementCounter(Data data, string key, bool decrement)
        {
            var counter =
                data.GetEnumeration<CounterDto>()
                    .Where(c => c.Key == key)
                    .OrderByDescending(c => c.Value)
                    .FirstOrDefault();
            if (counter != null)
            {
                counter.Value = counter.Value + (decrement ? -1 : 1);
            }
            else
            {
                counter = new CounterDto
                {
                    Id = AutoIncrementIdGenerator.GenerateId(typeof(ListDto)),
                    Key = key,
                    Value = (decrement ? 0 : 1)
                };

                data.Create(counter);
            }
            return counter;
        }

        public static void ArgumentNotNull(object argument, string name)
        {
            if (argument == null)
                throw new ArgumentException("Argment " + name + " should not be null!");
        }

        public static void ArgumentCondition(bool condition, string name, string message)
        {
            if (condition)
                throw new ArgumentException(message);
        }
    }
}