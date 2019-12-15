using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Hexastore.Processor
{
    public class StoreOperationFactory : IStoreOperationFactory
    {
        private readonly ConcurrentDictionary<string, Mutex> _locks = new ConcurrentDictionary<string, Mutex>();

        public IDisposable Write(string storeId)
        {
            var storeLock = _locks.GetOrAdd(storeId, (x) =>
            {
                return new Mutex();
            });
            storeLock.WaitOne();
            return new StoreOperation(storeLock);
        }
    }

    public class StoreOperation : IDisposable
    {
        private readonly Mutex _storeLock;

        public StoreOperation(Mutex storeLock)
        {
            _storeLock = storeLock;
        }

        public void Dispose()
        {
            _storeLock.ReleaseMutex();
        }
    }
}
