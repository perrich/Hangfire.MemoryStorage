using System;
using System.Linq;
using System.Threading;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.Server;

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
                int removedCount;

                do
                {
                    var table = Data.GetExpirables(t);
                    var data = (from d in table
                        where d.ExpireAt < now
                        orderby d.ExpireAt
                        select d).Take(NumberOfRecordsInSinglePass).ToList();

                    removedCount = data.Count;

                    if (removedCount == 0)
                    {
                        continue;
                    }

                    if (data is IIntIdentifiedData)
                        Data.Delete(t, (IIntIdentifiedData) data);
                    else if (data is IStringIdentifiedData)
                        Data.Delete(t, (IStringIdentifiedData) data);

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