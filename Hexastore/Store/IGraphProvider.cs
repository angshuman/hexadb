using Hexastore.Graph;

namespace Hexastore.Store
{
    /// <summary>
    /// Interface to store and retrieve graphs
    /// </summary>
    public interface IGraphProvider
    {
        IStoreGraph CreateGraph(string id, GraphType type);
        IStoreGraph GetGraph(string id, GraphType type);
        bool ContainsGraph(string id, GraphType type);
        bool DeleteGraph(string id, GraphType type);
        void WriteKey(string id, string key, string value);
        string ReadKey(string id, string key);
    }

    public enum GraphType
    {
        Data,
        Meta,
        Infer,
        Checkpoint
    }
}
