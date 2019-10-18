using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using Hexastore.Graph;
using RocksDbSharp;

namespace Hexastore.Rocks
{
    public class RocksGraph : IStoreGraph
    {
        private readonly string _name;
        private readonly RocksDb _db;
        private static readonly WriteOptions _writeOptions = (new WriteOptions()).SetSync(false);

        public RocksGraph(string name, RocksDb db)
        {
            _name = name;
            _db = db;
        }

        public string Name => _name;

        public int Count => throw new System.NotImplementedException();

        public bool Assert(Triple t)
        {
            if (Exists(t)) {
                return false;
            }
            var (sh, ph, ih, oh) = TripleHash(t.Subject, t.Predicate, t.Object);
            var (sKey, pKey, oKey) = GetKeys(sh, ph, ih, oh);

            var serializedTriple = t.ToBytes();
            using (var batch = new WriteBatch()) {
                batch.Put(sKey, serializedTriple);
                batch.Put(pKey, serializedTriple);
                batch.Put(oKey, serializedTriple);
                _db.Write(batch, _writeOptions);
            }
            return true;
        }

        public void Assert(IEnumerable<Triple> triples)
        {
            using (var batch = new WriteBatch()) {
                foreach (var t in triples) {
                    if (Exists(t)) {
                        continue;
                    }
                    var (sh, ph, ih, oh) = TripleHash(t.Subject, t.Predicate, t.Object);
                    var (sKey, pKey, oKey) = GetKeys(sh, ph, ih, oh);

                    var serializedTriple = t.ToBytes();
                    batch.Put(sKey, serializedTriple);
                    batch.Put(pKey, serializedTriple);
                    batch.Put(oKey, serializedTriple);
                }
                _db.Write(batch, _writeOptions);
            }
        }

        public bool Assert(string s, string p, TripleObject o)
        {
            return Assert(new Triple(s, p, o));
        }

        public void BatchRetractAssert(IEnumerable<Triple> retract, IEnumerable<Triple> assert)
        {
            using (var batch = new WriteBatch()) {
                foreach (var t in retract) {
                    var (sh, ph, ih, oh) = TripleHash(t.Subject, t.Predicate, t.Object);
                    var (sKey, pKey, oKey) = GetKeys(sh, ph, ih, oh);
                    batch.Delete(sKey);
                    batch.Delete(pKey);
                    batch.Delete(oKey);
                }
                foreach (var t in assert) {
                    var (sh, ph, ih, oh) = TripleHash(t.Subject, t.Predicate, t.Object);
                    var (sKey, pKey, oKey) = GetKeys(sh, ph, ih, oh);
                    var serializedTriple = t.ToBytes();
                    batch.Put(sKey, serializedTriple);
                    batch.Put(pKey, serializedTriple);
                    batch.Put(oKey, serializedTriple);
                }
                _db.Write(batch, _writeOptions);
            }
        }

        public object Clone()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
        }

        public bool Exists(Triple t)
        {
            return Exists(t.Subject, t.Predicate, t.Object);
        }

        public bool Exists(string s, string p, TripleObject o)
        {
            var (sh, ph, ih, oh) = TripleHash(s, p, o);
            var (sKey, pKey, oKey) = GetKeys(sh, ph, ih, oh);

            var SKey = KeyConfig.ConcatBytes(GetBytes($"{_name}.S"), KeyConfig.ByteZero, sh, KeyConfig.ByteZero, ph, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh);

            return _db.Get(SKey) != null ? true : false;
        }

        public IEnumerable<IGrouping<string, IGrouping<string, TripleObject>>> GetGroupings()
        {
            var nameBytes = GetBytes($"{_name}.S");
            var start = KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteOne);
            var subjects = new RocksEnumerable(_db, start, end, (it) =>
            {
                var key = it.Key();
                var splits = KeyConfig.Split(key);
                var nextKey = KeyConfig.ConcatBytes(splits[0], KeyConfig.ByteZero, splits[1], KeyConfig.ByteOne);
                return it.Seek(nextKey);
            }).Select(x => x.Subject);

            foreach (var s in subjects) {
                var sBytes = GetBytes($"{_name}.S");
                var sh = Hash(s);
                var startS = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero);
                var endS = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteOne);

                yield return new RocksSubjectGrouping(_db, _name, s, startS, endS);
            }
        }

        public IEnumerable<IGrouping<string, TripleObject>> GetSubjectGroupings(string s)
        {
            var sBytes = GetBytes($"{_name}.S");
            var sh = Hash(s);
            var startS = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteOne);
            return new RocksSubjectGrouping(_db, _name, s, startS, endS);
        }

        public IEnumerable<Triple> GetTriples()
        {
            var sBytes = GetBytes($"{_name}.S");
            var start = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, start, end, (Iterator it) => { return it.Next(); });
        }

        public IGraph Merge(IGraph g)
        {
            Assert(g.GetTriples());
            return this;
        }

        public IGraph Minus(IGraph g)
        {
            foreach (var t in g.GetTriples()) {
                Retract(t);
            }
            return this;
        }

        public IEnumerable<Triple> S(string s)
        {
            var sBytes = GetBytes($"{_name}.S");
            var sh = Hash(s);
            var start = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, start, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> S(string s, Triple c)
        {
            var sBytes = GetBytes($"{_name}.S");
            var sh = Hash(s);
            var start = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteOne);

            var (csh, cph, cih, coh) = TripleHash(c.Subject, c.Predicate, c.Object);

            var continuation = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, csh, KeyConfig.ByteZero, cph, KeyConfig.ByteZero, cih, KeyConfig.ByteZero, coh, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, start) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, end) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> SP(string s, string p)
        {
            var sBytes = GetBytes($"{_name}.S");
            var sh = Hash(s);
            var ph = Hash(p);
            var start = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero, ph, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero, ph, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, start, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> SP(string s, string p, Triple c)
        {
            var sBytes = GetBytes($"{_name}.S");
            var sh = Hash(s);
            var ph = Hash(p);
            var start = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero, ph, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, sh, KeyConfig.ByteZero, ph, KeyConfig.ByteOne);

            var (csh, cph, cih, coh) = TripleHash(c.Subject, c.Predicate, c.Object);

            var continuation = KeyConfig.ConcatBytes(sBytes, KeyConfig.ByteZero, csh, KeyConfig.ByteZero, cph, KeyConfig.ByteZero, cih, KeyConfig.ByteZero, coh, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, start) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, end) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> O(TripleObject o)
        {
            var oBytes = GetBytes($"{_name}.O");
            var ih = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            var oh = Hash(o.ToValue());

            var start = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, start, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> O(TripleObject o, Triple c)
        {
            if (c == null) {
                return O(o);
            }
            var oBytes = GetBytes($"{_name}.O");
            var ih = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            var oh = Hash(o.ToValue());

            var start = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteOne);

            var (csh, cph, cih, coh) = TripleHash(c.Subject, c.Predicate, c.Object);
            var continuation = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, cih, KeyConfig.ByteZero, coh, KeyConfig.ByteZero, csh, KeyConfig.ByteZero, cph, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, start) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, end) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> OS(TripleObject o, string s)
        {
            var oBytes = GetBytes($"{_name}.O");
            var ih = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            var oh = Hash(o.ToValue());
            var sh = Hash(s);

            var start = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero, sh, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero, sh, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, start, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> OS(TripleObject o, string s, Triple c)
        {
            if (c == null) {
                return OS(o, s);
            }
            var oBytes = GetBytes($"{_name}.O");
            var ih = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            var oh = Hash(o.ToValue());
            var sh = Hash(s);

            var start = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero, sh, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero, sh, KeyConfig.ByteOne);

            var (csh, cph, cih, coh) = TripleHash(c.Subject, c.Predicate, c.Object);
            var continuation = KeyConfig.ConcatBytes(oBytes, KeyConfig.ByteZero, cih, KeyConfig.ByteZero, coh, KeyConfig.ByteZero, csh, KeyConfig.ByteZero, cph, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, start) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, end) > 0) {
                return Enumerable.Empty<Triple>();
            }
            return new RocksEnumerable(_db, continuation, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> P(string p)
        {
            var pBytes = GetBytes($"{_name}.P");
            var ph = Hash(p);

            var start = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, ph, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, ph, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, start, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> P(string p, Triple c)
        {
            if (c == null) {
                return P(p);
            }
            var pBytes = GetBytes($"{_name}.P");
            var ph = Hash(p);

            var start = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, ph, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, ph, KeyConfig.ByteOne);

            var (csh, cph, cih, coh) = TripleHash(c.Subject, c.Predicate, c.Object);
            var continuation = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, cph, KeyConfig.ByteZero, cih, KeyConfig.ByteZero, coh, KeyConfig.ByteZero, csh, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, start) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, end) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> PO(string p, TripleObject o)
        {
            var pBytes = GetBytes($"{_name}.P");
            var ph = Hash(p);
            var ih = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            var oh = Hash(o.ToValue());

            var start = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, ph, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, ph, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, start, end, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> PO(string p, TripleObject o, Triple c)
        {
            if (c == null) {
                return PO(p, o);
            }
            var pBytes = GetBytes($"{_name}.P");
            var ph = Hash(p);
            var ih = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            var oh = Hash(o.ToValue());

            var start = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, ph, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, ph, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteOne);

            var (csh, cph, cih, coh) = TripleHash(c.Subject, c.Predicate, c.Object);
            var continuation = KeyConfig.ConcatBytes(pBytes, KeyConfig.ByteZero, cph, KeyConfig.ByteZero, cih, KeyConfig.ByteZero, coh, KeyConfig.ByteZero, csh, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, start) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, end) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, end, (Iterator it) => { return it.Next(); });
        }

        public bool Retract(Triple t)
        {
            return Retract(t.Subject, t.Predicate, t.Object);
        }

        public void Retract(IEnumerable<Triple> triples)
        {
            using (var batch = new WriteBatch()) {
                foreach (var t in triples) {
                    var (sh, ph, ih, oh) = TripleHash(t.Subject, t.Predicate, t.Object);
                    var (sKey, pKey, oKey) = GetKeys(sh, ph, ih, oh);
                    batch.Delete(sKey);
                    batch.Delete(pKey);
                    batch.Delete(oKey);
                }
                _db.Write(batch, _writeOptions);
            }
        }

        public bool Retract(string s, string p, TripleObject o)
        {
            if (!Exists(s, p, o)) {
                return false;
            }
            var (sh, ph, ih, oh) = TripleHash(s, p, o);
            var (sKey, pKey, oKey) = GetKeys(sh, ph, ih, oh);
            using (var batch = new WriteBatch()) {
                batch.Delete(sKey);
                batch.Delete(pKey);
                batch.Delete(oKey);
                _db.Write(batch, _writeOptions);
            }
            return true;
        }

        private byte[] Hash(string input)
        {
            return KeyConfig.Hash(input);
        }

        private (byte[], byte[], byte[], byte[]) TripleHash(string s, string p, TripleObject o)
        {
            return (Hash(s), Hash(p), o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse, Hash(o.ToValue()));
        }

        private static byte[] GetBytes(string str)
        {
            // todo: optimize GetBytes for known patterns
            return Encoding.UTF8.GetBytes(str);
        }

        private static string GetString(byte[] bytes)
        {
            return Encoding.UTF8.GetString(bytes);
        }

        private (byte[], byte[], byte[]) GetKeys(byte[] sh, byte[] ph, byte[] ih, byte[] oh)
        {
            var SKey = KeyConfig.ConcatBytes(GetBytes($"{_name}.S"), KeyConfig.ByteZero, sh, KeyConfig.ByteZero, ph, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh);
            var PKey = KeyConfig.ConcatBytes(GetBytes($"{_name}.P"), KeyConfig.ByteZero, ph, KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero, sh);
            var OKey = KeyConfig.ConcatBytes(GetBytes($"{_name}.O"), KeyConfig.ByteZero, ih, KeyConfig.ByteZero, oh, KeyConfig.ByteZero, sh, KeyConfig.ByteZero, ph);

            return (SKey, PKey, OKey);
        }
    }
}
