using Hexastore.Graph;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hexastore.Parser
{
    public class TripleConverter
    {
        public static IEnumerable<Triple> FromJson(JObject obj)
        {
            var graph = new List<Triple>();

            AddObject(graph, null, null, obj);
            return graph;
        }

        public static JObject ToJson(string id, IGraph graph)
        {
            var po = graph.GetSubjectGroupings(id);
            var rsp = new JObject { [Constants.ID] = id };
            if (!po.Any()) {
                return rsp;
            }

            foreach (var p in po) {
                var toList = new List<JToken>();
                foreach (var o in p) {
                    if (o.IsID) {
                        var obj = ToJson(o.ToValue(), graph);
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

        public static Patch ToPatch(IGraph graph, string id, JObject body)
        {
            var triplePatch = PatchObject(graph, id, body);
            return triplePatch;
        }

        public static JObject ToJson(IGraph graph)
        {
            var rsp = new JArray();
            foreach (var g in graph.GetGroupings()) {
                rsp.Add(ToJson(g.Key, graph));
            }
            return new JObject { ["@graph"] = rsp };
        }

        private static Patch PatchObject(IGraph graph, string id, JObject body)
        {
            return null;
        }

        private static void AddObject(IList<Triple> graph, string sourceId, string p, JObject obj)
        {
            if (!obj.ContainsKey(Constants.ID)) {
                throw new InvalidOperationException("Cannot find id");
            }

            var currentId = (string)obj[Constants.ID];
            if (sourceId != null) {
                graph.Add(new Triple(sourceId, p, new TripleObject(currentId)));
            }

            foreach (var item in obj) {
                if (item.Key == Constants.ID) {
                    continue;
                }
                AddValue(graph, currentId, item.Key, item.Value, null);
            }
        }

        private static void AddValue(IList<Triple> graph, string s, string p, JToken token, int? index)
        {
            switch (token.Type) {
                case JTokenType.Object:
                    var jobj = (JObject)token;
                    if (!jobj.ContainsKey(Constants.ID)) {
                        if (index == null) {
                            jobj[Constants.ID] = $"{s}{Constants.LinkDelimeter}{p}";
                        } else {
                            jobj[Constants.ID] = $"{s}{Constants.LinkDelimeter}{p}{Constants.LinkDelimeter}{index}";
                        }
                    }
                    AddObject(graph, s, p, jobj);
                    break;
                case JTokenType.Array:
                    var count = 0;
                    foreach (var item in (JArray)token) {
                        AddValue(graph, s, p, item, count++);
                    }
                    break;
                default:
                    graph.Add(new Triple(s, p, new TripleObject((JValue)token)));
                    break;
            }
        }
    }
}
