using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Hexastore.Graph;
using RocksDbSharp;

namespace Hexastore.Rocks
{
    public class RocksSubjectGrouping : IGrouping<string, IGrouping<string, TripleObject>>
    {
        private readonly RocksDb _db;
        private readonly string _name;
        private readonly string _s;
        private readonly byte[] _start;
        private readonly byte[] _end;
        private readonly int _prefixLength;

        public RocksSubjectGrouping(RocksDb db, string name, string s, byte[] start, byte[] end)
        {
            _db = db;
            _name = name;
            _s = s;
            _start = start;
            _end = end;
            _prefixLength = name.Length + 3;
        }

        public string Key => _s;

        public IEnumerator GetEnumerator()
        {
            throw new System.NotImplementedException();
        }

        IEnumerator<IGrouping<string, TripleObject>> IEnumerable<IGrouping<string, TripleObject>>.GetEnumerator()
        {
            var predicates = new RocksEnumerable(_db, _start, _end, (it) =>
            {
                var key = it.Key();
                var splits = KeyConfig.Split(key);
                var nextKey = KeyConfig.ConcatBytes(splits[0], KeyConfig.ByteZero, splits[1], KeyConfig.ByteZero, splits[2], KeyConfig.ByteOne);
                return it.Seek(nextKey);
            }).Select(x => x.Predicate);

            foreach (var p in predicates) {
                yield return new RocksPredicateGrouping(_db, _name, _s, p);
            }
        }
    }
}
