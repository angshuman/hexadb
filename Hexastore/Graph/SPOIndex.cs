using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Parser;
using Newtonsoft.Json.Linq;

namespace Hexastore.Graph
{
    public class SPOIndex
    {
        private readonly IDictionary<string, IDictionary<string, IList<TripleObject>>> _spo;

        public SPOIndex()
        {
            _spo = new Dictionary<string, IDictionary<string, IList<TripleObject>>>();
        }

        public void Assert(Triple t)
        {
            Assert(t.Subject, t.Predicate, t.Object);
        }

        public void Assert(IEnumerable<Triple> triples)
        {
            foreach (var t in triples) {
                Assert(t.Subject, t.Predicate, t.Object);
            }
        }

        public void Assert(string s, string p, TripleObject o)
        {
            Assert(_spo, s, p, o);
        }

        public JObject ToJson(string id, HashSet<string> seen = null)
        {
            if (seen == null) {
                seen = new HashSet<string>();
            }

            var po = GetSubjectGroupings(id);
            var rsp = new JObject { [Constants.ID] = id };
            if (!po.Any()) {
                return rsp;
            }

            if (seen.Contains(id)) {
                return rsp;
            }

            seen.Add(id);
            foreach (var p in po) {
                var toList = new List<JToken>();
                foreach (var o in p) {
                    if (o.IsID) {
                        var obj = ToJson(o.ToValue(), seen);
                        toList.Add(obj);
                    } else {
                        toList.Add(o.ToTypedJSON());
                    }
                }
                if (toList.Count == 1) {
                    rsp[p.Key] = toList.First();
                } else {
                    rsp[p.Key] = new JArray(toList);
                }
            }
            return rsp;
        }

        private static void Assert<TX, TY, TZ>(IDictionary<TX, IDictionary<TY, IList<TZ>>> xyz, TX tx, TY ty, TZ tz)
        {
            if (!xyz.TryGetValue(tx, out IDictionary<TY, IList<TZ>> yz)) {
                yz = new Dictionary<TY, IList<TZ>>();
                xyz[tx] = yz;
            }

            if (!yz.TryGetValue(ty, out IList<TZ> z)) {
                z = new List<TZ>();
                yz[ty] = z;
            }
            z.Add(tz);
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

        private class SubjectGrouping : IGrouping<string, IGrouping<string, TripleObject>>
        {
            private readonly KeyValuePair<string, IDictionary<string, IList<TripleObject>>> _kv;

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
            private readonly KeyValuePair<string, IList<TripleObject>> _kv;

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
