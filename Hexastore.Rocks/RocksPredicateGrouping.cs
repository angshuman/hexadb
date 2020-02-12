using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Hexastore.Graph;
using RocksDbSharp;

namespace Hexastore.Rocks
{
    internal class RocksPredicateGrouping : IGrouping<string, TripleObject>
    {
        private readonly RocksDb _db;
        private readonly string _s;
        private readonly string _p;
        private readonly string _name;

        public RocksPredicateGrouping(RocksDb db, string name, string s, string p)
        {
            _db = db;
            _s = s;
            _p = p;
            _name = name;
        }

        public string Key => _p;

        public IEnumerator<TripleObject> GetEnumerator()
        {
            var sBytes = Hash($"{_name}.S");
            var sh = Hash(_s);
            var ph = Hash(_p);
            var start = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero, ph, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero, ph, KeyConfig.ByteOne);

            var baseKey = $"{_name}.S.{Hash(_s)}.{Hash(_p)}";
            IEnumerable<Triple> idEnum = new RocksEnumerable(_db, start, end, (iterator) =>
            {
                return iterator.Next();
            }, (iterator) => { return iterator.IteratorToTriple(); });

            foreach (var item in idEnum) {
                yield return item.Object;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new System.NotImplementedException();
        }

        private byte[] Hash(string input)
        {
            return KeyConfig.GetBytes(input);
        }
    }
}