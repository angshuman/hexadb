using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Hexastore.Graph
{
    public class DisjointUnion : IGraph
    {
        private readonly IGraph _read;
        private readonly IGraph _write;

        public DisjointUnion(IGraph read, IGraph write, string name)
        {
            _read = read;
            _write = write;
            Name = name;
        }

        public int Count
        {
            get
            {
                return _read.Count + _write.Count;
            }
        }

        public string Name
        {
            get;
            private set;
        }

        public bool Assert(Triple t)
        {
            return Assert(t.Subject, t.Predicate, t.Object);
        }

        public bool Assert(string s, string p, TripleObject o)
        {
            if (_read.Exists(s, p, o)) {
                return false;
            }

            return _write.Assert(s, p, o);
        }

        public object Clone()
        {
            throw new NotImplementedException();
        }

        public bool Exists(Triple t)
        {
            if (_read.Exists(t) || _write.Exists(t)) {
                return true;
            }
            return false;
        }

        public bool Exists(string s, string p, TripleObject o)
        {
            if (_read.Exists(s, p, o) || _write.Exists(s, p, o)) {
                return true;
            }
            return false;
        }

        public IEnumerable<Triple> O(TripleObject o)
        {
            return _read.O(o).Concat(_write.O(o));
        }

        public IEnumerable<Triple> OS(TripleObject o, string s)
        {
            return _read.OS(o, s).Concat(_write.OS(o, s));
        }

        public IEnumerable<Triple> P(string p)
        {
            return _read.P(p).Concat(_write.P(p));
        }

        public IEnumerable<Triple> PO(string p, TripleObject o)
        {
            return _read.PO(p, o).Concat(_write.PO(p, o));
        }

        public IEnumerable<Triple> S(string s)
        {
            return _read.S(s).Concat(_write.S(s));
        }

        public IEnumerable<Triple> SP(string s, string p)
        {
            return _read.SP(s, p).Concat(_write.SP(s, p));
        }

        public IEnumerable<Triple> GetTriples()
        {
            return _read.GetTriples().Concat(_write.GetTriples());
        }

        public IGraph Merge(IGraph g)
        {
            throw new NotImplementedException();
        }

        public IGraph Minus(IGraph g)
        {
            throw new NotImplementedException();
        }

        public bool Retract(Triple t)
        {
            throw new NotImplementedException();
        }

        public void Retract(IEnumerable<Triple> triples)
        {
            throw new NotImplementedException();
        }

        public bool Retract(string s, string p, TripleObject o)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IGrouping<string, IGrouping<string, TripleObject>>> GetGroupings()
        {
            return _read.GetGroupings().Concat(_write.GetGroupings());
        }

        public IEnumerable<IGrouping<string, TripleObject>> GetSubjectGroupings(string s)
        {
            return _read.GetSubjectGroupings(s).Concat(_write.GetSubjectGroupings(s));
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
            return 2;
        }

        public void Dispose()
        {
        }

        public IEnumerable<bool> Assert(IEnumerable<Triple> triples)
        {
            throw new NotImplementedException();
        }

        void IGraph.Assert(IEnumerable<Triple> triples)
        {
            throw new NotImplementedException();
        }

        public void BatchRetractAssert(IEnumerable<Triple> retract, IEnumerable<Triple> assert)
        {
            throw new NotImplementedException();
        }
    }
}
