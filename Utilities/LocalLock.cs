using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Hangfire.MemoryStorage.Utilities
{
    public class LocalLock : IDisposable
    {
        private static readonly ConcurrentDictionary<string, object> Locks = new ConcurrentDictionary<string, object>();
        private readonly object _lock;

        private LocalLock(string resource, TimeSpan timeout)
        {
            _lock = Locks.GetOrAdd(resource, new object());

            Monitor.TryEnter(_lock, timeout);
        }

        public void Dispose()
        {
            Monitor.Exit(_lock);
        }

        public static IDisposable AcquireLock(string resource, TimeSpan timeout)
        {
            return new LocalLock(resource, timeout);
        }
    }
}