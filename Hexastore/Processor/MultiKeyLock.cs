using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace Hexastore.Processor
{
    public class MultiKeyLock : IDisposable
    {
        private readonly ConcurrentDictionary<string, byte> _locks;

        private readonly string[] _keys;

        public MultiKeyLock(string[] keys, ConcurrentDictionary<string, byte> locks)
        {
            _keys = keys;
            _locks = locks;
        }

        public void Dispose()
        {
            foreach (var key in _keys)
            {
                if (!_locks.TryRemove(key, out _))
                {
                    throw new Exception("Could not remove key from lock");
                }
            }
        }
    }
}
