using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Graph;
using Newtonsoft.Json.Linq;

namespace Hexastore.Query
{
    public class JsonQuery
    {
        public static IEnumerable<Triple> MakeQuery(JObject query, IGraph graph)
        {
            IEnumerable<Triple> response = null;
            foreach (var property in query) {
                if (property.Key == "hs:continuation") {

                }
                switch (property.Value.Type) {
                    case JTokenType.String:
                    case JTokenType.Float:
                    case JTokenType.Integer:
                    case JTokenType.Date:
                    case JTokenType.Uri:
                        if (response == null) {
                            response = graph.PO(property.Key, property.Value);
                        } else {
                            response = AddStringConstraint(graph, response, property);
                        }
                        break;
                    case JTokenType.Object:
                        var subQuery = MakeQuery((JObject)property.Value, graph);
                        if (response == null) {
                            if (subQuery.Any()) {
                                response = graph.PO(property.Key, subQuery.First().Subject);
                                response = subQuery.Skip(1).Aggregate(response, (r, x) => r.Union(graph.PO(property.Key, x.Subject)));
                            } else {
                                return Enumerable.Empty<Triple>();
                            }
                        } else {
                            if (subQuery.Any()) {
                                response = subQuery.Aggregate(response, (r, x) => r.Union(graph.PO(property.Key, x.Subject)));
                            } else {
                                return Enumerable.Empty<Triple>();
                            }
                        }
                        break;
                }
            }
            return response;
        }

        private static IEnumerable<Triple> AddStringConstraint(IGraph graph, IEnumerable<Triple> filter, KeyValuePair<string, JToken> property)
        {
            return filter.Where(x => graph.Exists(x.Subject, property.Key, TripleObject.FromData(property.Value.Value<string>())));
        }

        private static IEnumerable<Triple> AddIdConstraint(IGraph graph, IEnumerable<Triple> filter, string predicate, string subject)
        {
            return filter.Where(x => graph.Exists(x.Subject, predicate, subject));
        }
    }
}
