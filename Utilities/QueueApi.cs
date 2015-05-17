using System.Collections.Generic;
using System.Linq;
using Hangfire.MemoryStorage.Database;
using Hangfire.MemoryStorage.Dto;
using Hangfire.MemoryStorage.Entities;

namespace Hangfire.MemoryStorage.Utilities
{
    public static class QueueApi
    {
        public static IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage, bool fetched)
        {
            var jobs = Data.GetEnumeration<JobDto>();
            var queues = Data.GetEnumeration<JobQueueDto>();

            var ids = (from q in queues
                join j in jobs on q.JobId equals j.Id
                where q.Queue == queue && fetched == q.FetchedAt.HasValue
                select j.Id).Skip(from).Take(perPage).ToList();

            return ids;
        }

        public static EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(IEnumerable<JobQueueDto> allQueues,
            string queue)
        {
            var jobQueues = allQueues.Where(q => q.Queue == queue).Select(a => a.FetchedAt).ToList();
            var fetchedCount = jobQueues.Count(q => q.HasValue);

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = jobQueues.Count() - fetchedCount,
                FetchedCount = fetchedCount
            };
        }
    }
}