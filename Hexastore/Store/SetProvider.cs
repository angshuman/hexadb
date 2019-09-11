using Hexastore.Store;
using System;
using System.Collections.Concurrent;

public class SetProvider : IStoreProvider, IDisposable
{
    private readonly IGraphProvider _graphProvider;
    private readonly ConcurrentDictionary<string, IStore> _setList;

    public SetProvider(IGraphProvider graphProvider)
    {
        _graphProvider = graphProvider;
        _setList = new ConcurrentDictionary<string, IStore>();
    }

    public IStore GetOrAdd(string setId)
    {
        return _setList.GetOrAdd(setId, (key) => Store.Create(key, _graphProvider));
    }

    public bool Contains(string id)
    {
        return _setList.ContainsKey(id);
    }

    public IStore GetSet(string id)
    {
        return _setList[id];
    }

    public void Dispose()
    {
        foreach (var item in _setList) {
            item.Value.Dispose();
        }
    }
}