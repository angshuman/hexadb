using System;
using System.Collections.Generic;
using System.Text;

namespace Hexastore.Graph
{
    public interface IPagableGraph
    {
        IEnumerable<Triple> S(string s, Triple continuation);
        IEnumerable<Triple> P(string p, Triple continuation);
        IEnumerable<Triple> O(TripleObject o, Triple continuation);

        IEnumerable<Triple> SP(string s, string p, Triple continution);
        IEnumerable<Triple> PO(string p, TripleObject o, Triple continuation);
        IEnumerable<Triple> OS(TripleObject o, string s, Triple continuation);
    }
}
