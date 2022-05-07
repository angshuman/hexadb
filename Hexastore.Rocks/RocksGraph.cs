using System;
using System.Collections.Generic;
using System.Linq;
using Hexastore.Graph;
using RocksDbSharp;

namespace Hexastore.Rocks
{
    public class RocksGraph : IStoreGraph
    {
        private readonly RocksDb _db;
        private static readonly WriteOptions _writeOptions = (new WriteOptions()).SetSync(false);

        public RocksGraph(string name, RocksDb db)
        {
            Name = name;
            _db = db;
        }

        public string Name { get; }

        public int Count => throw new System.NotImplementedException();

        public bool Assert(Triple t)
        {
            if (Exists(t)) {
                return false;
            }

            var keySegments = new KeySegments(Name, t.Subject, t.Predicate, t.Object);
            var (sKey, pKey, oKey) = keySegments.GetKeys();

            var serializedTripleObject = t.Object.ToBytes();
            using (var batch = new WriteBatch()) {
                batch.Put(sKey, serializedTripleObject);
                batch.Put(pKey, keySegments.Type);
                if (t.Object.IsID) {
                    batch.Put(oKey, keySegments.Type);
                }
                _db.Write(batch, _writeOptions);
            }
            return true;
        }

        public void Assert(IEnumerable<Triple> triples)
        {
            using (var batch = new WriteBatch()) {
                foreach (var t in triples) {
                    var keySegments = new KeySegments(Name, t.Subject, t.Predicate, t.Object);
                    var (sKey, pKey, oKey) = keySegments.GetKeys();
                    var serializedTripleObject = t.Object.ToBytes();

                    batch.Put(sKey, serializedTripleObject);
                    batch.Put(pKey, keySegments.Type);
                    if (t.Object.IsID) {
                        batch.Put(oKey, keySegments.Type);
                    }
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
                    var (sKey, pKey, oKey) = new KeySegments(Name, t.Subject, t.Predicate, t.Object).GetKeys();

                    batch.Delete(sKey);
                    batch.Delete(pKey);
                    if (t.Object.IsID) {
                        batch.Delete(oKey);
                    }
                }

                foreach (var t in assert) {
                    var keySegments = new KeySegments(Name, t.Subject, t.Predicate, t.Object);
                    var (sKey, pKey, oKey) = keySegments.GetKeys();
                    var serializedTripleObject = t.Object.ToBytes();

                    batch.Put(sKey, serializedTripleObject);
                    batch.Put(pKey, keySegments.Type);
                    if (t.Object.IsID) {
                        batch.Put(oKey, keySegments.Type);
                    }
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
            var keySegments = new KeySegments(Name, s, p, o);
            var pPrefix = keySegments.GetPPrefix();
            var start = KeyConfig.ConcatBytes(pPrefix, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(pPrefix, KeyConfig.ByteOne);
            var oEnumerable = new RocksEnumerable(_db, start, end, (it) => it.Next());
            return oEnumerable.Any(x => x.Predicate == p);
        }

        public IEnumerable<IGrouping<string, IGrouping<string, TripleObject>>> GetGroupings()
        {
            var nameBytes = KeySegments.GetNameSKey(Name);
            var start = KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteOne);
            var subjects = new RocksEnumerable(_db, start, end, (it) => {
                var key = it.Key();
                var splits = KeyConfig.Split(key);
                var nextKey = KeyConfig.ConcatBytes(splits[0], KeyConfig.ByteZero, splits[1], KeyConfig.ByteOne);
                return it.Seek(nextKey);
            }).Select(x => x.Subject);

            foreach (var s in subjects) {
                var sh = KeySegments.GetNameSKeySubject(Name, s);
                var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
                var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);

                yield return new RocksSubjectGrouping(_db, Name, s, startS, endS);
            }
        }

        public IEnumerable<IGrouping<string, TripleObject>> GetSubjectGroupings(string s)
        {
            var sh = KeySegments.GetNameSKeySubject(Name, s);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);
            return new RocksSubjectGrouping(_db, Name, s, startS, endS);
        }

        public IEnumerable<Triple> GetTriples()
        {
            var nameBytes = KeySegments.GetNameSKey(Name);
            var start = KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteOne);
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
            var sh = KeySegments.GetNameSKeySubject(Name, s);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> S(string s, Triple c)
        {
            var sh = KeySegments.GetNameSKeySubject(Name, s);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);

            // todo: optimize
            var (sKey, _, _) = new KeySegments(Name, c).GetKeys();

            var continuation = KeyConfig.ConcatBytes(sKey, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, startS) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, endS) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> SP(string s, string p)
        {
            var sh = KeySegments.GetNameSKeySubjectPredicate(Name, s, p);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public Triple SPI(string s, string p, int index)
        {
            var sh = KeySegments.GetNameSKeySubjectPredicateIndex(Name, s, p, index);
            var toBytes = _db.Get(sh);
            if (toBytes == null) {
                return null;
            }
            var to = toBytes.ToTripleObject();
            return new Triple(s, p, new TripleObject(to.Value, to.IsID, to.TokenType, index));
        }

        public IEnumerable<Triple> SP(string s, string p, Triple c)
        {
            var sh = KeySegments.GetNameSKeySubjectPredicate(Name, s, p);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);

            // todo: optimize
            var (sKey, _, _) = new KeySegments(Name, c).GetKeys();
            var continuation = KeyConfig.ConcatBytes(sKey, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, startS) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, endS) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> O(TripleObject o)
        {
            var oh = KeySegments.GetNameOKeyObject(Name, o);
            var startS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteOne);

            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> O(TripleObject o, Triple c)
        {
            if (c == null) {
                return O(o);
            }

            var oh = KeySegments.GetNameOKeyObject(Name, o);
            var startS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteOne);

            var (_, _, oKey) = new KeySegments(Name, c).GetKeys();
            var continuation = KeyConfig.ConcatBytes(oKey, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, startS) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, endS) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> OS(TripleObject o, string s)
        {
            var oh = KeySegments.GetNameOKeyObjectSubject(Name, o, s);
            var startS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteOne);

            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> OS(TripleObject o, string s, Triple c)
        {
            if (c == null) {
                return OS(o, s);
            }
            var oh = KeySegments.GetNameOKeyObjectSubject(Name, o, s);
            var startS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteOne);

            var (_, _, oKey) = new KeySegments(Name, c).GetKeys();
            var continuation = KeyConfig.ConcatBytes(oKey, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, startS) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, endS) > 0) {
                return Enumerable.Empty<Triple>();
            }
            return new RocksEnumerable(_db, continuation, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> P(string p)
        {
            var ph = KeySegments.GetNamePKeyPredicate(Name, p);
            var startS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteOne);

            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<string> P()
        {
            var pPrefix = KeySegments.GetNamePPredicate(Name);
            var startP = KeyConfig.ConcatBytes(pPrefix, KeyConfig.ByteZero);
            var endP = KeyConfig.ConcatBytes(pPrefix, KeyConfig.ByteOne);
            var predicates = new RocksEnumerable(_db, startP, endP, (it) => {
                var key = it.Key();
                var splits = KeyConfig.Split(key);
                var nextKey = KeyConfig.ConcatBytes(splits[0], KeyConfig.ByteZero, splits[1], KeyConfig.ByteOne);
                return it.Seek(nextKey);
            }).Select(x => x.Predicate);

            return predicates;
        }

        public IEnumerable<Triple> P(string p, Triple c)
        {
            if (c == null) {
                return P(p);
            }
            var ph = KeySegments.GetNamePKeyPredicate(Name, p);
            var startS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteOne);

            var (_, pKey, _) = new KeySegments(Name, c).GetKeys();
            var continuation = KeyConfig.ConcatBytes(pKey, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, startS) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, endS) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> PO(string p, TripleObject o)
        {
            var ph = KeySegments.GetNamePKeyPredicateObject(Name, p, o);
            var startS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteOne);

            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> PO(string p, TripleObject o, Triple c)
        {
            if (c == null) {
                return PO(p, o);
            }
            var ph = KeySegments.GetNamePKeyPredicateObject(Name, p, o);
            var startS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteOne);

            var (_, pKey, _) = new KeySegments(Name, c).GetKeys();
            var continuation = KeyConfig.ConcatBytes(pKey, KeyConfig.ByteOne);

            if (KeyConfig.ByteCompare(continuation, startS) < 0) {
                throw new InvalidOperationException("Invalid continuation token. Before range");
            } else if (KeyConfig.ByteCompare(continuation, endS) > 0) {
                return Enumerable.Empty<Triple>();
            }

            return new RocksEnumerable(_db, continuation, endS, (Iterator it) => { return it.Next(); });
        }

        public bool Retract(Triple t)
        {
            return Retract(t.Subject, t.Predicate, t.Object);
        }

        public void Retract(IEnumerable<Triple> triples)
        {
            using (var batch = new WriteBatch()) {
                foreach (var t in triples) {
                    var (sKey, pKey, oKey) = new KeySegments(Name, t).GetKeys();
                    batch.Delete(sKey);
                    batch.Delete(pKey);
                    if (t.Object.IsID) {
                        batch.Delete(oKey);
                    }
                }
                _db.Write(batch, _writeOptions);
            }
        }

        public bool Retract(string s, string p, TripleObject o)
        {
            if (!Exists(s, p, o)) {
                return false;
            }
            var (sKey, pKey, oKey) = new KeySegments(Name, s, p, o).GetKeys();
            using (var batch = new WriteBatch()) {
                batch.Delete(sKey);
                batch.Delete(pKey);
                if (o.IsID) {
                    batch.Delete(oKey);
                }
                _db.Write(batch, _writeOptions);
            }
            return true;
        }
    }
}
