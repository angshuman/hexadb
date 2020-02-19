using System;
using System.Collections;
using System.Collections.Generic;
using Hexastore.Graph;
using RocksDbSharp;

namespace Hexastore.Rocks
{
    public class RocksEnumerable : IEnumerable<Triple>
    {
        private readonly RocksDb _db;
        private readonly byte[] _start;
        private readonly byte[] _end;
        private readonly Func<Iterator, Iterator> _nextFunction;

        public RocksEnumerable(RocksDb db, byte[] start, byte[] end, Func<Iterator, Iterator> nextFunction)
        {
            _db = db;
            _start = start;
            _end = end;
            _nextFunction = nextFunction;
        }

        public IEnumerator GetEnumerator()
        {
            return new RocksEnumerator(_db, _start, _end, _nextFunction);
        }

        IEnumerator<Triple> IEnumerable<Triple>.GetEnumerator()
        {
            return new RocksEnumerator(_db, _start, _end, _nextFunction);
        }
    }

    public class RocksEnumerator : IEnumerator<Triple>
    {
        private readonly RocksDb _db;
        private readonly byte[] _start;
        private readonly byte[] _end;
        private byte[] _currentKey;
        private readonly Func<Iterator, Iterator> _nextFunction;
        private Iterator _iterator;

        public RocksEnumerator(RocksDb db, byte[] start, byte[] end, Func<Iterator, Iterator> nextFunction)
        {
            _db = db;
            _start = start;
            _end = end;
            _currentKey = null;
            _nextFunction = nextFunction;
        }

        public Triple Current
        {
            get
            {
                var t = (_iterator.Key(), _iterator.Value());
                return t.ToTriple();
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            if (_iterator != null) {
                _iterator.Dispose();
            }
        }

        public bool MoveNext()
        {
            if (_currentKey == null) {
                _currentKey = _start;
                // todo: check
                _iterator = _db.NewIterator();
                _iterator.Seek(_start);
                var firstKey = _iterator.Key();
                if (KeyConfig.ByteCompare(firstKey, _start) < 0) {
                    return false;
                }
            } else {
                _nextFunction(_iterator);
                _currentKey = _iterator.Key();
            }

            if (!_iterator.Valid()) {
                return false;
            }

            var key = _iterator.Key();
            if (KeyConfig.ByteCompare(key, _end) > 0) {
                return false;
            }
            return true;
        }

        public void Reset()
        {
            _currentKey = null;
        }
    }
}
