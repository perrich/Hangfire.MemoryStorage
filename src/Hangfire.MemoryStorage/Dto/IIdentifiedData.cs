using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hangfire.MemoryStorage.Dto
{
    public interface IIdentifiedData<T>
    {
        T Id { get; }
    }
}
