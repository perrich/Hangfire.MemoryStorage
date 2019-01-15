using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.Storage;

namespace Hangfire.MemoryStorage
{
    public class MemoryStorageFetchedJob : IFetchedJob
    {
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;
        private readonly Data _data; 

        public MemoryStorageFetchedJob(Data data, JobQueueDto queue)
        {
            Id = queue.Id;
            JobId = queue.JobId;
            _data = data;
        }

        public int Id { get; private set; }
        public string JobId { get; private set; }

        public void RemoveFromQueue()
        {
            var queue = _data.Get<JobQueueDto>(Id);
            if (queue != null)
            {
                _data.Delete(queue);
            }

            _removedFromQueue = true;
        }

        public void Requeue()
        {
            var queue = _data.Get<JobQueueDto>(Id);
            if (queue != null)
            {
                queue.FetchedAt = null;
            }

            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }
    }
}