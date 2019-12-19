using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Hexastore.Graph;

namespace Hexastore.Processor
{
    public class MultiKeyLockFactory : IMultiKeyLockFactory
    {
        private readonly ConcurrentDictionary<string, byte> _locks;
        private readonly int _timeoutMs;

        public MultiKeyLockFactory(ConcurrentDictionary<string, byte> locks, int timeoutMs)
        {
            _locks = locks;
            _timeoutMs = timeoutMs;
        }

        public MultiKeyLockFactory() : this(new ConcurrentDictionary<string, byte>(), 10_000)
        {
        }

        private string[] getKeys(string storeId, IEnumerable<Triple> triples)
        {
            var SPs = new Dictionary<string, Triple>();
            
            foreach (var triple in triples)
            {
                SPs[$"{storeId}.{triple.Subject}.{triple.Predicate}"] = triple;
            }

            return SPs.Keys.ToArray();
        }

        public IDisposable WriteLock(string storeId, IEnumerable<Triple> triples)
        {
            var stack = new ConcurrentStack<string>();
            var timer = Stopwatch.StartNew();
            var succeeded = false;
            var keys = getKeys(storeId, triples);
            while (timer.ElapsedMilliseconds <= _timeoutMs && !succeeded)
            {
                succeeded = true;
                foreach (var key in keys)
                {
                    if (!_locks.TryAdd(key, 0))
                    {
                        //roll back
                        while (stack.TryPop(out var keyToRemove))
                        {
                            if (!_locks.TryRemove(keyToRemove, out _))
                            {
                                throw new Exception("Could not remove key from lock during rollback");
                            }
                        }
                        Console.WriteLine($"Failed to get lock for key {key}");
                        Thread.Sleep(1);
                        succeeded = false;
                        break;
                    }

                    stack.Push(key);
                }
            }

            if (succeeded)
            {
                return new MultiKeyLock(keys, _locks);
            }

            throw new TimeoutException("Couldn't retrieve locks before timeout");
        }
    }

   
}
