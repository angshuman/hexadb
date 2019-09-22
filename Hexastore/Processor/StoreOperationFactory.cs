using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Hexastore.Processor
{
    public class StoreOperationFactory : IStoreOperationFactory
    {
        private readonly ConcurrentDictionary<string, ReaderWriterLock> _locks = new ConcurrentDictionary<string, ReaderWriterLock>();

        public IDisposable Read(string storeId)
        {
            var storeLock = _locks.GetOrAdd(storeId, (x) =>
            {
                return new ReaderWriterLock();
            });
            storeLock.AcquireReaderLock(10_000);
            return new StoreOperation(storeLock);
        }

        public IDisposable Write(string storeId)
        {
            var storeLock = _locks.GetOrAdd(storeId, (x) =>
            {
                return new ReaderWriterLock();
            });
            storeLock.AcquireWriterLock(10_000);
            return new StoreOperation(storeLock);
        }
    }

    public class StoreOperation : IDisposable
    {
        private readonly ReaderWriterLock _storeLock;

        public StoreOperation(ReaderWriterLock storeLock)
        {
            _storeLock = storeLock;
        }

        public void Dispose()
        {
            _storeLock.ReleaseLock();
        }
    }
}
