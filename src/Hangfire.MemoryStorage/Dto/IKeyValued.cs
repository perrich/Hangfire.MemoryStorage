namespace Hangfire.MemoryStorage.Dto
{
    public interface IKeyValued
    {
        string Key { get; }
        string Value { get; }
    }
}