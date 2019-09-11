using System;
using System.Collections.Generic;
using System.Linq;

namespace Hexastore.Graph
{
    public interface IGraph : ICloneable, IDisposable
    {
        /// <summary>
        /// Gets the Name of the graph
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the Count of triples in this graph - this should be efficient
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Assert adds a triple to the graph if its not already there
        /// </summary>
        bool Assert(Triple t);
        void Assert(IEnumerable<Triple> triples);

        /// <summary>
        /// Assert adds a triple to the graph if its not already there
        /// </summary>
        bool Assert(string s, string p, TripleObject o);

        /// <summary>
        /// Merge another graph into this one: Assert all the triples in that graph into this graph
        /// </summary>
        IGraph Merge(IGraph g);

        /// <summary>
        /// Retracts a triple from the graph if its there
        /// </summary>
        bool Retract(Triple t);
        void Retract(IEnumerable<Triple> t);

        /// <summary>
        /// Retracts a triple from the graph if its there
        /// </summary>
        bool Retract(string s, string p, TripleObject o);

        /// <summary>
        /// Subtracts a whole graph from the graph: Retract all the triples in that graph from this graph
        /// </summary>
        IGraph Minus(IGraph g);

        /// <summary>
        /// S returns all the triples with this subject
        /// </summary>
        IEnumerable<Triple> S(string s);

        /// <summary>
        /// P returns all the triples with this predicate
        /// </summary>
        IEnumerable<Triple> P(string p);

        /// <summary>
        /// O returns all the triples with this object
        /// </summary>
        IEnumerable<Triple> O(TripleObject o);

        /// <summary>
        /// SP returns all the triples with this subject and this predicate
        /// </summary>
        IEnumerable<Triple> SP(string s, string p);

        /// <summary>
        /// SP returns all the triples with this predicate and this object
        /// </summary>
        IEnumerable<Triple> PO(string p, TripleObject o);

        /// <summary>
        /// OS returns all the triples with this object and subject
        /// </summary>
        IEnumerable<Triple> OS(TripleObject o, string s);

        /// <summary>
        /// Returns all the triples
        /// </summary>
        IEnumerable<Triple> GetTriples();

        /// <summary>
        /// Tests whether the triple exists in the graph
        /// </summary>
        bool Exists(Triple t);

        /// <summary>
        /// Tests whether the triple exists in the graph
        /// </summary>
        bool Exists(string s, string p, TripleObject o);

        /// <summary>
        /// Efficiently enumerate over the whole graph
        /// </summary>
        IEnumerable<IGrouping<string, IGrouping<string, TripleObject>>> GetGroupings();

        /// <summary>
        /// Efficiently enumerate over a particular subject
        /// </summary>
        IEnumerable<IGrouping<string, TripleObject>> GetSubjectGroupings(string s);
    }
}
