using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Hexastore.Graph
{
    public class GraphOperator
    {
        public static IEnumerable<Triple> Path(IGraph graph, string id, params string[] paths)
        {
            return PathImpl(graph, id, paths);
        }

        public static IEnumerable<TripleObject> PathObjects(IGraph graph, string id, params string[] paths)
        {
            return PathImpl(graph, id, paths).Select(x => x.Object);
        }

        public static IEnumerable<Triple> Expand(IGraph graph, string id, int level, params string[] expand)
        {
            if (level == 0) {
                yield break;
            }
            var triples = graph.S(id).ToList();
            foreach (var t in triples) {
                yield return t;
                if (t.Object.IsID && HasValue(t.Predicate, expand)) {
                    var rsp = Expand(graph, t.Object.Id, level - 1, expand);
                    foreach (var expanded in rsp) {
                        yield return expanded;
                    }
                }
            }
        }

        private static IEnumerable<Triple> PathImpl(IGraph graph, string id, string[] paths)
        {
            if (paths.Length == 1) {
                var destination = paths[0];
                var rsp = graph.SP(id, destination);
                return rsp;
            }

            var destinations = graph.SP(id, paths[0]).Where(x => x.Object.IsID).Select(x => x.Object.Id);
            var newPaths = paths.Skip(1).ToArray();
            var all = new List<Triple>();
            foreach (var item in destinations) {
                all.AddRange(PathImpl(graph, item, newPaths));
            }
            return all;
        }

        private static bool HasValue(string item, IEnumerable<string> list)
        {
            if (list == null || !list.Any()) {
                return true;
            }
            return list.Contains(item);
        }
    }
}
