namespace Hexastore.Store
{
    public interface IStoreProvider
    {
        bool Contains(string id);
        IStore GetOrAdd(string setId);
        IStore GetSet(string id);
    }
}