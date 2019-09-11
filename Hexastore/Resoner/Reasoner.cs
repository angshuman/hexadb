using Hexastore.Graph;

namespace Hexastore.Resoner
{
    public class Reasoner : IReasoner
    {
        public void Spin(IGraph data, IGraph infer, IGraph meta)
        {
            var union = new DisjointUnion(data, infer, string.Empty);
            bool any = false;
            do {
                any = false;
                any |= InverseOf(union, meta);
                any |= Domain(union, meta);
                any |= Range(union, meta);
                any |= SubClassOf(union, meta);
                any |= SubPropertyOf(union, meta);
            } while (any);
        }

        private bool Range(DisjointUnion graph, IGraph meta)
        {
            var any = false;
            var rules = meta.P(Constants.Range);
            foreach (var rule in rules) {
                var triples = graph.P(rule.Subject);
                foreach (var t in triples) {
                    any |= graph.Assert(t.Object.ToValue(), Constants.Type, TripleObject.FromData(rule.Object.ToValue()));
                }
            }
            return any;
        }

        private bool Domain(DisjointUnion graph, IGraph meta)
        {
            var any = false;
            var rules = meta.P(Constants.Domain);
            foreach (var rule in rules) {
                var triples = graph.P(rule.Subject);
                foreach (var t in triples) {
                    any |= graph.Assert(t.Subject, Constants.Type, TripleObject.FromData(rule.Object.ToValue()));
                }
            }
            return any;
        }

        private bool InverseOf(IGraph graph, IGraph meta)
        {
            var any = false;
            var rules = meta.P(Constants.InverseOf);
            foreach (var rule in rules) {
                var triples = graph.P(rule.Subject);
                foreach (var t in triples) {
                    any |= graph.Assert(t.Object.ToValue(), rule.Object.ToValue(), t.Subject);
                }
            }
            return any;
        }

        private bool SubClassOf(IGraph graph, IGraph meta)
        {
            var any = false;
            var rules = meta.P(Constants.SubClassOf);
            foreach (var rule in rules) {
                var triples = graph.PO(Constants.Type, TripleObject.FromData(rule.Subject));
                foreach (var t in triples) {
                    any |= graph.Assert(t.Subject, Constants.Type, TripleObject.FromData(rule.Object.ToValue()));
                }
            }
            return any;
        }

        private bool SubPropertyOf(IGraph graph, IGraph meta)
        {
            var any = false;
            var rules = meta.P(Constants.SubPropertyOf);
            foreach (var rule in rules) {
                var triples = graph.P(rule.Subject);
                foreach (var t in triples) {
                    any |= graph.Assert(t.Subject, rule.Object.ToValue(), t.Object);
                }
            }
            return any;
        }
    }
}
