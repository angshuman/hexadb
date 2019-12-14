using System;
using System.Collections.Generic;
using Hexastore.Graph;

namespace Hexastore.Processor
{
    public interface IMultiKeyLockFactory
    {
        IDisposable WriteLock(string storeId, IEnumerable<Triple> triples);
    }
}