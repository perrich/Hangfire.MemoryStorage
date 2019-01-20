using System;
using System.Linq;
using System.Threading;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.Server;
using System.Collections.Generic;
using System.Reflection;

namespace Hangfire.MemoryStorage
{
#pragma warning disable 618
    public class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private readonly Data _data;

        private static readonly Type[] Types =
        {
            typeof (AggregatedCounterDto),
            typeof (JobDto),
            typeof (ListDto),
            typeof (SetDto),
            typeof (HashDto)
        };

        private readonly TimeSpan _checkInterval;

        public ExpirationManager(Data data, TimeSpan checkInterval)
        {
            _data = data;
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            var now = DateTime.UtcNow;

            foreach (var t in Types.Select(t => t.GetTypeInfo()))
            {
                if (!typeof(IExpirable).GetTypeInfo().IsAssignableFrom(t))
                    continue;

                int removedCount;

                do
                {
                    var table = _data.GetEnumeration(t.AsType());
                    var data = (from d in table
                        where ((IExpirable)d).ExpireAt < now
                        select d).Take(NumberOfRecordsInSinglePass).ToList();

                    removedCount = data.Count;

                    if (removedCount == 0)
                    {
                        continue;
                    }

                    if (typeof(IIdentifiedData<int>).GetTypeInfo().IsAssignableFrom(t))
                    {
                        _data.Delete(data.Cast<IIdentifiedData<int>>());
                    }
                    else if (typeof(IIdentifiedData<string>).GetTypeInfo().IsAssignableFrom(t))
                    {
                        _data.Delete(data.Cast<IIdentifiedData<string>>());
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