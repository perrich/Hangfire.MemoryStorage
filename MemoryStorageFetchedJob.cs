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

        public MemoryStorageFetchedJob(JobQueueDto queue)
        {
            Id = queue.Id;
            JobId = queue.JobId;
        }

        public int Id { get; private set; }
        public string JobId { get; private set; }

        public void RemoveFromQueue()
        {
            var queue = Data.Get<JobQueueDto>(Id);
            if (queue != null)
            {
                Data.Delete(typeof (JobQueueDto), queue);
            }

            _removedFromQueue = true;
        }

        public void Requeue()
        {
            var queue = Data.Get<JobQueueDto>(Id);
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