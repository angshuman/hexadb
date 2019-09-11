using Hexastore.Graph;
using System;
using System.Collections.Generic;

namespace Hexastore.Store
{
    public class Store : IStore, IDisposable
    {
        private readonly string _id;
        private readonly IGraphProvider _provider;
        private readonly Dictionary<GraphType, IStoreGraph> _graphs = new Dictionary<GraphType, IStoreGraph>();

        public static IStore Create(string id, IGraphProvider provider)
        {
            return new Store(id, provider);
        }

        public Store(string id, IGraphProvider provider)
        {
            _id = id;
            _provider = provider;
            _graphs[GraphType.Meta] = provider.CreateGraph(id, GraphType.Meta);
            _graphs[GraphType.Data] = provider.CreateGraph(id, GraphType.Data);
            _graphs[GraphType.Infer] = provider.CreateGraph(id, GraphType.Infer);
        }

        public void Dispose()
        {
            _graphs[GraphType.Meta].Dispose();
            _graphs[GraphType.Meta].Dispose();
            _graphs[GraphType.Meta].Dispose();
        }

        public IStoreGraph GetGraph(GraphType type)
        {
            return _graphs[type];
        }
    }
}
