namespace Hangfire.MemoryStorage.Dto
{
    public class JobParameterDto : IIntIdentifiedData
    {
        public string JobId { get; set; }
        public string Name { get; set; }
        public string Value { get; set; }
        public int Id { get; set; }
    }
}