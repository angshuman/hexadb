using System;
using System.Threading.Tasks;

namespace Hexastore.Processor
{
    public interface IStoreOperationFactory
    {
        IDisposable Write(string storeId);
    }
}