using System;
using System.Collections.Generic;

namespace Hangfire.MemoryStorage.Database
{
    public static class AutoIncrementIdGenerator
    {
        private static readonly Dictionary<Type, int> Identifiers = new Dictionary<Type, int>();
        private static readonly object LockObject = new object();

        public static int GenerateId(Type type)
        {
            int result;
            lock (LockObject)
            {
                if (!Identifiers.ContainsKey(type))
                {
                    result = 1;
                    Identifiers.Add(type, result);
                }
                result = Identifiers[type]++;
            }

            return result;
        }
    }
}