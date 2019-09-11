using Hexastore.Graph;
using System;
using System.Collections.Generic;

namespace Hexastore.Store
{
    public class MemoryGraphProvider : IGraphProvider
    {
        private readonly object _lockObject = new object(); // todo: this needs to be per app
        private readonly IDictionary<string, IStoreGraph> _store;

        public MemoryGraphProvider()
        {
            _store = new Dictionary<string, IStoreGraph>();
        }

        public bool ContainsGraph(string id, GraphType type)
        {
            return _store.ContainsKey(MakeKey(id, type));
        }

        public IStoreGraph CreateGraph(string id, GraphType type)
        {
            string key = MakeKey(id, type);
            lock (_lockObject) {
                if (!_store.ContainsKey(key)) {
                    _store[key] = new MemoryGraph();
                    return _store[key];
                }
                throw new InvalidOperationException($"Graph exists {id} {type}");
            }
        }

        public bool DeleteGraph(string id, GraphType type)
        {
            string key = MakeKey(id, type);
            if (!_store.ContainsKey(key)) {
                _store[key] = null;
                return true;
            }
            return false;
        }

        public IStoreGraph GetGraph(string id, GraphType type)
        {
            string key = MakeKey(id, type);
            if (!_store.ContainsKey($"{Constants.Data}{id}")) {
                throw new InvalidOperationException($"Cannot find graph id:{id} type:{type}");
            }
            return _store[key];
        }

        public string ReadKey(string key)
        {
            throw new NotImplementedException();
        }

        public void WriteKey(string key, string value)
        {
            throw new NotImplementedException();
        }

        private string MakeKey(string id, GraphType type)
        {
            return $"{type}:{id}"; ;
        }
    }
}
