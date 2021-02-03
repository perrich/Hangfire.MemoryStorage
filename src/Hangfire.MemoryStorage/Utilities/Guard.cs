using System;

namespace Hangfire.MemoryStorage.Utilities
{
    public static class Guard
    {
        public static void ArgumentNotNull(object argument, string name)
        {
            if (argument == null)
                throw new ArgumentException("Argument " + name + " should not be null!");
        }

        public static void ArgumentCondition(bool condition, string name, string message)
        {
            if (condition)
                throw new ArgumentException(message);
        }
    }
}
