using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Hangfire.MemoryStorage.Utilities
{
    public class LocalLock : IDisposable
    {
        private static readonly ConcurrentDictionary<string, object> Locks = new ConcurrentDictionary<string, object>();
        private readonly object _lock;
        private readonly string _resource;

        private LocalLock(string resource, TimeSpan timeout)
        {
            _lock = Locks.GetOrAdd(resource, new object());
            _resource = resource;

            bool hasEntered = Monitor.TryEnter(_lock, timeout);
            if (!hasEntered)
            {
                throw new SynchronizationLockException();
            }
        }

        public void Dispose()
        {
            Monitor.Exit(_lock);

            Locks.TryRemove(_resource, out object value);
        }

        public static IDisposable AcquireLock(string resource, TimeSpan timeout)
        {
            return new LocalLock(resource, timeout);
        }
    }
}
