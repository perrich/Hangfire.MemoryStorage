using System;
using System.Linq;
using System.Threading;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.Server;
using System.Collections.Generic;

namespace Hangfire.MemoryStorage
{
    public class ExpirationManager : IServerComponent
    {
        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);

        private static readonly Type[] Types =
        {
            typeof (AggregatedCounterDto),
            typeof (JobDto),
            typeof (ListDto),
            typeof (SetDto),
            typeof (HashDto)
        };

        private readonly TimeSpan _checkInterval;

        public ExpirationManager(TimeSpan checkInterval)
        {
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var now = DateTime.UtcNow;

            foreach (var t in Types)
            {
                if (!typeof(IExpirable).IsAssignableFrom(t))
                    continue;

                int removedCount;

                do
                {
                    var table = Data.GetEnumeration(t);
                    var data = (from d in table
                        where ((IExpirable)d).ExpireAt < now
                        select d).Take(NumberOfRecordsInSinglePass).ToList();

                    removedCount = data.Count;

                    if (removedCount == 0)
                    {
                        continue;
                    }

                    if (typeof(IIdentifiedData<int>).IsAssignableFrom(t))
                    {
                        Data.Delete(data.Cast<IIdentifiedData<int>>());
                    }
                    else if (typeof(IIdentifiedData<string>).IsAssignableFrom(t))
                    {
                        Data.Delete(data.Cast<IIdentifiedData<string>>());
                    }

                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                } while (removedCount != 0);
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }

        public override string ToString()
        {
            return "Composite C1 Records Expiration Manager";
        }
    }
}