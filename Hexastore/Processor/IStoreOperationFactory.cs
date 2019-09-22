using System;

namespace Hexastore.Processor
{
    public interface IStoreOperationFactory
    {
        IDisposable Read(string storeId);
        IDisposable Write(string storeId);
    }
}