using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Hexastore.Graph
{
    /// <summary>
    /// In-memory fully-indexed named triple set. i.e. a fully query-able graph structure
    /// </summary>
    public class MemoryGraph : IStoreGraph
    {
        private readonly IDictionary<string, IDictionary<string, IList<TripleObject>>> _spo;
        private readonly IDictionary<string, IDictionary<TripleObject, IList<string>>> _pos;
        private readonly IDictionary<TripleObject, IDictionary<string, IList<string>>> _osp;

        public MemoryGraph(string name = null)
        {
            // todo: cheap and dirty for supporting IList
            Name = name ?? string.Empty;
            _spo = new Dictionary<string, IDictionary<string, IList<TripleObject>>>();
            _pos = new Dictionary<string, IDictionary<TripleObject, IList<string>>>();
            _osp = new Dictionary<TripleObject, IDictionary<string, IList<string>>>();
        }

        public string Name
        {
            get;
            private set;
        }

        public int Count
        {
            get;
            private set;
        }

        public bool Assert(Triple t)
        {
            return Assert(t.Subject, t.Predicate, t.Object);
        }

        public IEnumerable<bool> Assert(IEnumerable<Triple> triples)
        {
            foreach (var t in triples) {
                yield return Assert(t.Subject, t.Predicate, t.Object);
            }
        }

        public bool Assert(string s, string p, TripleObject o)
        {
            if (s == null || p == null || o == null || o.IsNull) {
                return false;
            }

            if (Assert(_spo, s, p, o)) {
                if (Assert(_pos, p, o, s)) {
                    if (Assert(_osp, o, s, p)) {
                        Count++;
                        return true;
                    }
                }
            }
            return false;
        }

        public IGraph Merge(IGraph g)
        {
            foreach (var t in g.GetTriples()) {
                Assert(t);
            }
            return this;
        }

        public IGraph Minus(IGraph g)
        {
            foreach (var t in g.GetTriples()) {
                Retract(t);
            }
            return this;
        }

        public bool Retract(Triple t)
        {
            return Retract(t.Subject, t.Predicate, t.Object);
        }

        public void Retract(IEnumerable<Triple> triples)
        {
            foreach (var t in triples) {
                Retract(t);
            }
        }

        public bool Retract(string s, string p, TripleObject o)
        {
            if (s == null || p == null || o == null || o.IsNull) {
                return false;
            }

            if (Retract(_spo, s, p, o)) {
                if (Retract(_pos, p, o, s)) {
                    if (Retract(_osp, o, s, p)) {
                        Count--;
                        return true;
                    }
                }
            }
            return false;
        }

        public IEnumerable<Triple> O(TripleObject to)
        {
            IDictionary<string, IList<string>> sp;
            if (_osp.TryGetValue(to, out sp)) {
                foreach (var s in sp) {
                    foreach (var p in s.Value) {
                        yield return new Triple(s.Key, p, to);
                    }
                }
            }
        }

        public IEnumerable<Triple> OS(TripleObject to, string ts)
        {
            IDictionary<string, IList<string>> sp;
            if (_osp.TryGetValue(to, out sp)) {
                IList<string> p;
                if (sp.TryGetValue(ts, out p)) {
                    foreach (var tp in p) {
                        yield return new Triple(ts, tp, to);
                    }
                }
            }
        }

        public IEnumerable<Triple> P(string tp)
        {
            IDictionary<TripleObject, IList<string>> os;
            if (_pos.TryGetValue(tp, out os)) {
                foreach (var o in os) {
                    foreach (var s in o.Value) {
                        yield return new Triple(s, tp, o.Key);
                    }
                }
            }
        }

        public IEnumerable<Triple> PO(string tp, TripleObject to)
        {
            IDictionary<TripleObject, IList<string>> os;
            if (_pos.TryGetValue(tp, out os)) {
                IList<string> s;
                if (os.TryGetValue(to, out s)) {
                    foreach (var ts in s) {
                        yield return new Triple(ts, tp, to);
                    }
                }
            }
        }

        public IEnumerable<Triple> S(string ts)
        {
            IDictionary<string, IList<TripleObject>> po;
            if (_spo.TryGetValue(ts, out po)) {
                foreach (var o in po) {
                    foreach (var qo in o.Value) {
                        yield return new Triple(ts, o.Key, qo);
                    }
                }
            }
        }

        public IEnumerable<Triple> SP(string ts, string tp)
        {
            IDictionary<string, IList<TripleObject>> po;
            if (_spo.TryGetValue(ts, out po)) {
                IList<TripleObject> o;
                if (po.TryGetValue(tp, out o)) {
                    foreach (var qo in o) {
                        yield return new Triple(ts, tp, qo);
                    }
                }
            }
        }

        public IEnumerable<Triple> GetTriples()
        {
            foreach (var s in _spo) {
                foreach (var po in s.Value) {
                    foreach (var o in po.Value) {
                        yield return new Triple(s.Key, po.Key, o);
                    }
                }
            }
        }

        public bool Exists(Triple t)
        {
            return Exists(t.Subject, t.Predicate, t.Object);
        }

        public bool Exists(string ts, string tp, TripleObject to)
        {
            IDictionary<string, IList<TripleObject>> po;
            if (_spo.TryGetValue(ts, out po)) {
                IList<TripleObject> o;
                if (po.TryGetValue(tp, out o)) {
                    return o.Contains(to);
                }
            }
            return false;
        }

        public object Clone()
        {
            var result = new MemoryGraph();
            foreach (var t in GetTriples()) {
                result.Assert(t);
            }
            return result;
        }

#if DEBUG
        public override string ToString()
        {
            var sb = new StringBuilder();
            foreach (var t in GetTriples()) {
                sb.AppendLine(t.ToString());
            }
            return sb.ToString();
        }
#endif

        public override bool Equals(object obj)
        {
            if (obj is IGraph) {
                return Equals(this, (IGraph)obj);
            }
            return false;
        }

        public override int GetHashCode()
        {
            return 2; //todo: fix this
        }

        private static bool Equals(IGraph x, IGraph y)
        {
            if (ReferenceEquals(x, y)) {
                return true;
            }

            if (x == null || y == null || x.Count != y.Count) {
                return false;
            }

            foreach (var tx in x.GetTriples()) {
                if (!y.Exists(tx)) {
                    return false;
                }
            }

            return true;
        }

        private static bool Assert<TX, TY, TZ>(IDictionary<TX, IDictionary<TY, IList<TZ>>> xyz, TX tx, TY ty, TZ tz)
        {
            IDictionary<TY, IList<TZ>> yz;
            if (!xyz.TryGetValue(tx, out yz)) {
                yz = new Dictionary<TY, IList<TZ>>();
                xyz[tx] = yz;
            }
            IList<TZ> z;
            if (!yz.TryGetValue(ty, out z)) {
                z = new List<TZ>();
                yz[ty] = z;
            }
            // todo: dirty
            //if (z.Contains(tz)) {
            //    return false;
            //}
            z.Add(tz);
            return true;
        }

        private static bool Retract<TX, TY, TZ>(IDictionary<TX, IDictionary<TY, IList<TZ>>> xyz, TX tx, TY ty, TZ tz)
        {
            bool f = false;
            IDictionary<TY, IList<TZ>> yz;
            if (xyz.TryGetValue(tx, out yz)) {
                IList<TZ> z;
                if (yz.TryGetValue(ty, out z)) {
                    f = z.Remove(tz);
                    if (z.Count == 0) {
                        yz.Remove(ty);
                        if (yz.Count == 0) {
                            xyz.Remove(tx);
                        }
                    }
                }
            }
            return f;
        }

        public IEnumerable<IGrouping<string, IGrouping<string, TripleObject>>> GetGroupings()
        {
            foreach (var s in _spo) {
                yield return new SubjectGrouping(s);
            }
        }

        public IEnumerable<IGrouping<string, TripleObject>> GetSubjectGroupings(string s)
        {
            IDictionary<string, IList<TripleObject>> po;
            if (_spo.TryGetValue(s, out po)) {
                foreach (var p in po) {
                    yield return new PredicateGrouping(p);
                }
            }
        }

        public void Dispose()
        {
        }

        void IGraph.Assert(IEnumerable<Triple> triples)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<Triple> S(string s, Triple continuation)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<Triple> P(string p, Triple continuation)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<Triple> O(TripleObject o, Triple continuation)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<Triple> SP(string s, string p, Triple continution)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<Triple> PO(string p, TripleObject o, Triple continuation)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<Triple> OS(TripleObject o, string s, Triple continuation)
        {
            throw new System.NotImplementedException();
        }

        public void BatchRetractAssert(IEnumerable<Triple> retract, IEnumerable<Triple> assert)
        {
            Retract(retract);
            Assert(assert);
        }

        private class SubjectGrouping : IGrouping<string, IGrouping<string, TripleObject>>
        {
            private KeyValuePair<string, IDictionary<string, IList<TripleObject>>> _kv;

            public SubjectGrouping(KeyValuePair<string, IDictionary<string, IList<TripleObject>>> kv)
            {
                _kv = kv;
            }

            public string Key
            {
                get { return _kv.Key; }
            }

            public IEnumerator<IGrouping<string, TripleObject>> GetEnumerator()
            {
                foreach (var p in _kv.Value) {
                    yield return new PredicateGrouping(p);
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        private class PredicateGrouping : IGrouping<string, TripleObject>
        {
            private KeyValuePair<string, IList<TripleObject>> _kv;

            public PredicateGrouping(KeyValuePair<string, IList<TripleObject>> kv)
            {
                _kv = kv;
            }

            public string Key
            {
                get { return _kv.Key; }
            }

            public IEnumerator<TripleObject> GetEnumerator()
            {
                foreach (var o in _kv.Value) {
                    yield return o;
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
