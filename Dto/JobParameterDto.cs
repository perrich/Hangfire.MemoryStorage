namespace Hangfire.MemoryStorage.Dto
{
    public class JobParameterDto : IIdentifiedData<int>
    {
        public string JobId { get; set; }
        public string Name { get; set; }
        public string Value { get; set; }
        public int Id { get; set; }
    }
}