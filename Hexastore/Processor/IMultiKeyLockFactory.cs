using System;

namespace Hexastore.Processor
{
    public interface IMultiKeyLockFactory
    {
        IDisposable WriteLock(string[] keys);
    }
}