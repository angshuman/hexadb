using System;
using System.Collections.Generic;
using System.Linq;
using Hexastore.Errors;
using Hexastore.Graph;
using Hexastore.Parser;
using Hexastore.Query;
using Hexastore.Resoner;
using Hexastore.Store;
using Hexastore.Web.Errors;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Hexastore.Processor
{
    public class StoreProcessor : IStoreProcessor
    {
        private readonly IStoreProvider _setProvider;
        private readonly IReasoner _reasoner;
        private readonly StoreError _storeErrors;
        private readonly ILogger<StoreProcessor> _logger;

        public StoreProcessor(IStoreProvider setProvider, IReasoner reasoner, ILogger<StoreProcessor> logger)
        {
            _setProvider = setProvider;
            _reasoner = reasoner;
            _storeErrors = new StoreError();
            _logger = logger;
        }

        public void Assert(string storeId, JToken input, bool strict)
        {
            try {
                JArray value;
                if (input is JObject) {
                    value = new JArray(input);
                } else if (input is JArray) {
                    value = (JArray)input;
                } else {
                    throw _storeErrors.InvalidType;
                }

                var (data, _, _) = GetSetGraphs(storeId);
                foreach (var item in value) {
                    if (!(item is JObject)) {
                        throw _storeErrors.InvalidItem;
                    }
                    var jobj = (JObject)item;
                    if (!jobj.ContainsKey(Constants.ID) || jobj[Constants.ID].Type != JTokenType.String) {
                        throw _storeErrors.MustHaveId;
                    }
                    if (strict && data.S(jobj[Constants.ID].ToString()).Any()) {
                        throw new StoreException($"Already contains object with id {jobj[Constants.ID]}", _storeErrors.AlreadyContainsIdError);
                    }
                    var graph = TripleConverter.FromJson((JObject)item);
                    data.Assert(graph);
                }
            } catch (Exception e) {
                _logger.LogError("Assert failed. {Message}\n {StackTrace}", e.Message, e.StackTrace);
                throw e;
            }
        }

        public void PatchJson(string storeId, JToken token)
        {
            JArray inputs;
            if (token is JObject) {
                inputs = new JArray() { token };
            } else if (token is JArray) {
                inputs = token as JArray;
            } else {
                throw new InvalidOperationException("Invalid patch");
            }

            try {
                foreach (var input in inputs) {
                    if (!(input is JObject)) {
                        throw new InvalidOperationException("Invalid patch");
                    }
                    var patch = TripleConverter.FromJson(input as JObject);
                    var (data, _, _) = GetSetGraphs(storeId);
                    var retract = new List<Triple>();
                    foreach (var triple in patch) {
                        var t = data.SPI(triple.Subject, triple.Predicate, triple.Object.Index);
                        if (t != null) {
                            retract.Add(t);
                        }
                    }
                    var assert = patch.Where(x => !x.Object.IsNull);
                    data.BatchRetractAssert(retract, assert);
                }
            } catch (Exception e) {
                _logger.LogError("Patch JSON failed. {Message}\n {StackTrace}", e.Message, e.StackTrace);
                throw e;
            }
        }

        public void PatchTriple(string storeId, JObject input)
        {
            try {
                var (data, _, _) = GetSetGraphs(storeId);
                var remove = input["remove"];
                if (remove != null && remove is JObject) {
                    var triples = TripleConverter.FromJson((JObject)remove);
                    var toRemove = new List<Triple>();
                    foreach (var item in triples) {
                        if (item.Object.Index == -1) {
                            toRemove.Add(item);
                        } else {
                            var arrayMatches = data.SP(item.Subject, item.Predicate).Where(x => x.Object.Value == item.Object.Value && x.Object.IsID == item.Object.IsID);
                            toRemove.AddRange(arrayMatches);
                        }
                    }
                    data.Retract(toRemove);
                }
                var add = input["add"];
                if (add != null && add is JObject) {
                    var triples = TripleConverter.FromJson((JObject)add);
                    var toAssert = new List<Triple>();
                    var spCounter = new Dictionary<string, Dictionary<string, int>>();

                    foreach (var item in triples) {
                        if (item.Object.Index == -1) {
                            toAssert.Add(item);
                        } else {
                            if (!spCounter.ContainsKey(item.Subject) || !spCounter[item.Subject].ContainsKey(item.Predicate)) {
                                if (!spCounter.ContainsKey(item.Subject)) {
                                    spCounter[item.Subject] = new Dictionary<string, int>();
                                }
                                if (!spCounter[item.Subject].ContainsKey(item.Predicate)) {
                                    spCounter[item.Subject][item.Predicate] = -1;
                                    var arrayLast = data.SP(item.Subject, item.Predicate).LastOrDefault();
                                    spCounter[item.Subject][item.Predicate] = arrayLast == null ? -1 : arrayLast.Object.Index;
                                }
                            }
                            var arrayIndex = ++spCounter[item.Subject][item.Predicate];
                            toAssert.Add(new Triple(item.Subject, item.Predicate, new TripleObject(item.Object.Value, item.Object.IsID, item.Object.TokenType, arrayIndex)));
                        }
                    }
                    data.Assert(toAssert);
                }
            } catch (Exception e) {
                _logger.LogError("Patch triple failed. {Message}\n {StackTrace}", e.Message, e.StackTrace);
                throw e;
            }
        }

        public void Delete(string storeId, JToken token)
        {
            JArray inputs;
            try {
                if (token is JObject) {
                    inputs = new JArray() { token };
                } else if (token is JArray) {
                    inputs = token as JArray;
                } else {
                    throw new InvalidOperationException("Invalid delete");
                }
                foreach (var input in inputs) {
                    if (!(input is JObject)) {
                        throw new InvalidOperationException("Invalid delete");
                    }
                    var (data, _, _) = GetSetGraphs(storeId);
                    var triples = TripleConverter.FromJson(input as JObject);
                    data.Retract(triples);
                }
            } catch (Exception e) {
                _logger.LogError("Delete failed. {Message}\n {StackTrace}", e.Message, e.StackTrace);
            }
        }

        public void AssertMeta(string storeId, JObject value)
        {
            var (data, infer, meta) = GetSetGraphs(storeId);
            _reasoner.Spin(data, infer, meta);
        }

        public JObject GetSet(string storeId)
        {
            var (data, _, _) = GetSetGraphs(storeId);
            return TripleConverter.ToJson(data);
        }

        public (IStoreGraph, IStoreGraph, IStoreGraph) GetGraphs(string storeId)
        {
            // unsafe
            return GetSetGraphs(storeId);
        }

        public JObject GetSubject(string storeId, string subject, string[] expand, int level)
        {
            var (data, _, _) = GetSetGraphs(storeId);
            var triples = GraphOperator.Expand(data, subject, level, expand).ToList();
            if (triples.Count() == 0) {
                return null;
            }
            var rspGraph = new SPOIndex();
            rspGraph.Assert(triples);
            return rspGraph.ToJson(subject);
        }

        public JObject GetPredicates(string storeId)
        {
            var (data, _, _) = GetSetGraphs(storeId);
            var predicates = data.P().ToList();
            return new JObject {
                    new JProperty("values", new JArray(predicates))
                };
        }

        public JObject GetType(string storeId, string[] type, string[] expand, int level)
        {
            throw new NotImplementedException();
        }

        public JObject Query(string storeId, JObject query, string[] expand, int level)
        {
            try {
                var (data, _, _) = GetSetGraphs(storeId);
                ObjectQueryModel queryModel;
                try {
                    queryModel = query.ToObject<ObjectQueryModel>();
                } catch (Exception e) {
                    throw new StoreException(e.Message, _storeErrors.UnableToParseQuery);
                }

                var result = new ObjectQueryExecutor().Query(queryModel, data);
                dynamic response = new {
                    values = result.Values?.Select(x => {
                        var expanded = GraphOperator.Expand(data, x.Subject, level, expand);
                        var rspGraph = new SPOIndex();
                        rspGraph.Assert(expanded);
                        return rspGraph.ToJson(x.Subject);
                    }),
                    continuation = result.Continuation,
                    aggregates = result.Aggregates
                };
                return JObject.FromObject(response);
            } catch (Exception e) {
                _logger.LogError("Query failed. {Message}\n {StackTrace}", e.Message, e.StackTrace);
                throw e;
            }
        }

        private (IStoreGraph, IStoreGraph, IStoreGraph) GetSetGraphs(string storeId)
        {
            var set = _setProvider.GetOrAdd(storeId);
            var data = set.GetGraph(GraphType.Data);
            var infer = set.GetGraph(GraphType.Infer);
            var meta = set.GetGraph(GraphType.Meta);

            return (data, infer, meta);
        }

        public void CreateTwin(string storeId, JToken data)
        {
            data["$adt.type"] = "T";
            Assert(storeId, data, false);
        }

        public void CreateRelationship(string storeId, JToken data)
        {
            data["$adt.type"] = "R";
            Assert(storeId, data, false);
        }
    }
}
