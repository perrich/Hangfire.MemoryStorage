using System;

namespace Hangfire.MemoryStorage.Dto
{
    public interface IExpirable
    {
        DateTime? ExpireAt { get; }
    }
}