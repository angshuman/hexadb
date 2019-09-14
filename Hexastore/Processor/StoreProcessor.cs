using Hexastore.Graph;
using Hexastore.Parser;
using Hexastore.Query;
using Hexastore.Resoner;
using Hexastore.Store;
using Hexastore.Errors;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using Hexastore.Web.Errors;

namespace Hexastore.Processor
{
    public class StoreProcessor : IStoreProcesor
    {
        private readonly IStoreProvider _setProvider;
        private readonly IReasoner _reasoner;
        private readonly StoreError _storeErrors;

        public StoreProcessor(IStoreProvider setProvider, IReasoner reasoner)
        {
            _setProvider = setProvider;
            _reasoner = reasoner;
            _storeErrors = new StoreError();
        }

        public void Assert(string storeId, JToken input, bool strict)
        {
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
                    throw new StoreException($"Already contains object with id {jobj[Constants.ID]}", "409.001");
                    throw _storeErrors.MustHaveId;
                }

                var graph = TripleConverter.FromJson((JObject)item);
                data.Assert(graph);
            }
        }

        public void PatchJson(string storeId, JObject input)
        {
            var patches = TripleConverter.FromJson(input);
            var (data, _, _) = GetSetGraphs(storeId);
            var retract = new List<Triple>();
            foreach (var triple in patches) {
                retract.AddRange(data.SP(triple.Subject, triple.Predicate));
            }
            data.Retract(retract);
            var assert = patches.Where(x => !x.Object.IsNull).ToArray();
            data.Assert(assert);
        }

        public void PatchTriple(string storeId, JObject input)
        {
            var (data, _, _) = GetSetGraphs(storeId);
            var remove = input["remove"];
            if (remove != null && remove is JObject) {
                var triples = TripleConverter.FromJson((JObject)remove);
                data.Retract(triples);
            }
            var add = input["add"];
            if (add != null && add is JObject) {
                var triples = TripleConverter.FromJson((JObject)add);
                data.Assert(triples);
            }
        }

        public void AssertMeta(string storeId, JObject value)
        {
            var graph = TripleConverter.FromJson(value);
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
            return GetSetGraphs(storeId);
        }

        public JObject GetSubject(string storeId, string subject, string[] expand, int level)
        {
            var (data, _, _) = GetSetGraphs(storeId);
            var triples = GraphOperator.Expand(data, subject, level, expand).ToList();
            if (triples.Count() == 0) {
                return null;
            }
            var rspGraph = new MemoryGraph();
            rspGraph.Assert(triples).ToList();
            return TripleConverter.ToJson(subject, rspGraph);
        }

        public JObject GetType(string storeId, string[] type, string[] expand, int level)
        {
            throw new NotImplementedException();
        }

        public JObject Query(string storeId, JObject query, string[] expand, int level)
        {
            var (data, _, _) = GetSetGraphs(storeId);
            ObjectQueryModel queryModel;
            try {
                queryModel = query.ToObject<ObjectQueryModel>();
            } catch (Exception e) {
                throw new StoreException(e.Message, "400.009");
            }

            var result = new ObjectQueryExecutor().Query(queryModel, data);
            var response = new
            {
                values = result.Values.Select(x =>
                {
                    var expanded = GraphOperator.Expand(data, x.Subject, level, expand);
                    var rspGraph = new MemoryGraph();
                    rspGraph.Assert(expanded).ToList();
                    return TripleConverter.ToJson(x.Subject, rspGraph);
                }),
                result.Continuation
            };
            return JObject.FromObject(response);
        }

        private (IStoreGraph, IStoreGraph, IStoreGraph) GetSetGraphs(string storeId)
        {
            var set = _setProvider.GetOrAdd(storeId);
            var data = set.GetGraph(GraphType.Data);
            var infer = set.GetGraph(GraphType.Infer);
            var meta = set.GetGraph(GraphType.Meta);

            return (data, infer, meta);
        }
    }
}
