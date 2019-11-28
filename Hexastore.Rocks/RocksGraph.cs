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

            var (sKey, pKey, oKey) = new KeySegments(_name, t.Subject, t.Predicate, t.Object).GetKeys();

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

                    var (sKey, pKey, oKey) = new KeySegments(_name, t.Subject, t.Predicate, t.Object).GetKeys();
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
                    var (sKey, pKey, oKey) = new KeySegments(_name, t.Subject, t.Predicate, t.Object).GetKeys();

                    batch.Delete(sKey);
                    batch.Delete(pKey);
                    batch.Delete(oKey);
                }

                foreach (var t in assert) {
                    var (sKey, pKey, oKey) = new KeySegments(_name, t.Subject, t.Predicate, t.Object).GetKeys();
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
            var keySegments = new KeySegments(_name, s, p, o);
            var oPrefix = keySegments.GetOPrefix();
            var start = KeyConfig.ConcatBytes(oPrefix, KeyConfig.ByteZero);
            var end = KeyConfig.ConcatBytes(oPrefix, KeyConfig.ByteOne);
            var oEnumerable = new RocksEnumerable(_db, start, end, (it) => it.Next());
            return oEnumerable.Any();
        }

        public IEnumerable<IGrouping<string, IGrouping<string, TripleObject>>> GetGroupings()
        {
            var nameBytes = KeySegments.GetNameSKey(_name);
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
                var sh = KeySegments.GetNameSKeySubject(_name, s);
                var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
                var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);

                yield return new RocksSubjectGrouping(_db, _name, s, startS, endS);
            }
        }

        public IEnumerable<IGrouping<string, TripleObject>> GetSubjectGroupings(string s)
        {
            var sh = KeySegments.GetNameSKeySubject(_name, s);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);
            return new RocksSubjectGrouping(_db, _name, s, startS, endS);
        }

        public IEnumerable<Triple> GetTriples()
        {
            var nameBytes = KeySegments.GetNameSKey(_name);
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
            var sh = KeySegments.GetNameSKeySubject(_name, s);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> S(string s, Triple c)
        {
            var sh = KeySegments.GetNameSKeySubject(_name, s);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);

            // todo: optimize
            var (sKey, _, _) = new KeySegments(_name, c).GetKeys();

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
            var sh = KeySegments.GetNameSKeySubjectPredicate(_name, s, p);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);
            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> SP(string s, string p, Triple c)
        {
            var sh = KeySegments.GetNameSKeySubjectPredicate(_name, s, p);
            var startS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(sh, KeyConfig.ByteOne);

            // todo: optimize
            var (sKey, _, _) = new KeySegments(_name, c).GetKeys();
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
            var oh = KeySegments.GetNameOKeyObject(_name, o);
            var startS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteOne);

            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> O(TripleObject o, Triple c)
        {
            if (c == null) {
                return O(o);
            }

            var oh = KeySegments.GetNameOKeyObject(_name, o);
            var startS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteOne);

            var (_, _, oKey) = new KeySegments(_name, c).GetKeys();
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
            var oh = KeySegments.GetNameOKeyObjectSubject(_name, o, s);
            var startS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteOne);

            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> OS(TripleObject o, string s, Triple c)
        {
            if (c == null) {
                return OS(o, s);
            }
            var oh = KeySegments.GetNameOKeyObjectSubject(_name, o, s);
            var startS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(oh, KeyConfig.ByteOne);

            var (_, _, oKey) = new KeySegments(_name, c).GetKeys();
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
            var ph = KeySegments.GetNamePKeyPredicate(_name, p);
            var startS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteOne);

            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> P(string p, Triple c)
        {
            if (c == null) {
                return P(p);
            }
            var ph = KeySegments.GetNamePKeyPredicate(_name, p);
            var startS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteOne);

            var (_, pKey, _) = new KeySegments(_name, c).GetKeys();
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
            var ph = KeySegments.GetNamePKeyPredicateObject(_name, p, o);
            var startS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteOne);

            return new RocksEnumerable(_db, startS, endS, (Iterator it) => { return it.Next(); });
        }

        public IEnumerable<Triple> PO(string p, TripleObject o, Triple c)
        {
            if (c == null) {
                return PO(p, o);
            }
            var ph = KeySegments.GetNamePKeyPredicateObject(_name, p, o);
            var startS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteZero);
            var endS = KeyConfig.ConcatBytes(ph, KeyConfig.ByteOne);

            var (_, pKey, _) = new KeySegments(_name, c).GetKeys();
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
                    var (sKey, pKey, oKey) = new KeySegments(_name, t).GetKeys();
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
            var (sKey, pKey, oKey) = new KeySegments(_name, s, p, o).GetKeys();
            using (var batch = new WriteBatch()) {
                batch.Delete(sKey);
                batch.Delete(pKey);
                batch.Delete(oKey);
                _db.Write(batch, _writeOptions);
            }
            return true;
        }
    }
}
