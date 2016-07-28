using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Hangfire.MemoryStorage.Utilities
{
    public static class ConcurrentDictionaryExtensions
    {
        public static bool Remove<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> self, TKey key)
        {
            return ((IDictionary<TKey, TValue>) self).Remove(key);
        }
    }
}