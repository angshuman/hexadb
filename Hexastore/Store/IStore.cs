using Hexastore.Graph;
using System;

namespace Hexastore.Store
{
    public interface IStore : IDisposable
    {
        IStoreGraph GetGraph(GraphType type);
    }
}