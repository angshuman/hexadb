using Hexastore.Graph;
using Hexastore.Parser;
using Hexastore.Query;
using Hexastore.Resoner;
using Hexastore.Store;
using Hexastore.Errors;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Hexastore.Web.Errors;
using Microsoft.Extensions.Logging;

namespace Hexastore.Processor
{
    public class StoreProcessor : IStoreProcesor
    {
        private readonly IStoreProvider _setProvider;
        private readonly IReasoner _reasoner;
        private readonly StoreError _storeErrors;
        private readonly ILogger<StoreProcessor> _logger;
        private readonly IMultiKeyLockFactory _multiKeyLockFactory;

        public StoreProcessor(IStoreProvider setProvider, IReasoner reasoner, IMultiKeyLockFactory multiKeyLockFactory, ILogger<StoreProcessor> logger)
        {
            _setProvider = setProvider;
            _reasoner = reasoner;
            _storeErrors = new StoreError();
            _logger = logger;
            _multiKeyLockFactory = multiKeyLockFactory;
        }

        public void Assert(string storeId, JToken input, bool strict)
        {
            try
            {
                JArray value;
                if (input is JObject)
                {
                    value = new JArray(input);
                }
                else if (input is JArray)
                {
                    value = (JArray)input;
                }
                else
                {
                    throw _storeErrors.InvalidType;
                }

                foreach (var item in value)
                {
                    if (!(item is JObject))
                    {
                        throw _storeErrors.InvalidItem;
                    }
                    var jobj = (JObject)item;
                    if (!jobj.ContainsKey(Constants.ID) || jobj[Constants.ID].Type != JTokenType.String)
                    {
                        throw _storeErrors.MustHaveId;
                    }

                    var graph = TripleConverter.FromJson((JObject)item);

                    using (var op = _multiKeyLockFactory.WriteLock(storeId, graph))
                    {
                        var (data, _, _) = GetSetGraphs(storeId);
                        if (strict && data.S(jobj[Constants.ID].ToString()).Any()) {
                            throw new StoreException($"Already contains object with id {jobj[Constants.ID]}", _storeErrors.AlreadyContainsIdError);
                        }
                        data.Assert(graph);
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError("Assert failed. {Message}\n {StackTrace}", e.Message, e.StackTrace);
                throw e;
            }
        }

        public void PatchJson(string storeId, JToken token)
        {
            JArray inputs;

            if (token is JObject)
            {
                inputs = new JArray() { token };
            }
            else if (token is JArray)
            {
                inputs = token as JArray;
            }
            else
            {
                throw new InvalidOperationException("Invalid patch input");
            }

            var totalLockTime = 0l;
            var totalWaitTime = 0l;
            var totalTime = 0l;
            var totalRetractTime = 0l;
            var totalAssertTime = 0l;
            var totalBatchTime = 0l;

            try
            {
                var timer2 = Stopwatch.StartNew();

                var patches = new Dictionary<string, Triple>();
                foreach (var input in inputs)
                {
                    if (!(input is JObject))
                    {
                        throw new InvalidOperationException($"Invalid patch component '{input}'");
                    }

                    foreach (var triple in TripleConverter.FromJson(input as JObject))
                    {
                        patches[$"{storeId}.{triple.Subject}.{triple.Predicate}"] = triple;
                    }
                }

                var (data, _, _) = GetSetGraphs(storeId);
                var timer1 = Stopwatch.StartNew();
                using (var mlock = _multiKeyLockFactory.WriteLock(storeId, patches.Values))
                {
                    timer1.Stop();
                    var timer = Stopwatch.StartNew();
                    var retract = new List<Triple>();
                    var retractTimer = Stopwatch.StartNew();
                    foreach (var triple in patches.Values)
                    {
                        retract.AddRange(data.SP(triple.Subject, triple.Predicate));
                    }
                    totalRetractTime += retractTimer.ElapsedMilliseconds;

                    var assertTimer = Stopwatch.StartNew();
                    var assert = patches.Values.Where(x => !x.Object.IsNull).ToArray();
                    totalAssertTime += assertTimer.ElapsedMilliseconds;

                    var batchTimer = Stopwatch.StartNew();
                    //SNA
                    data.BatchRetractAssert(retract, assert);
                    totalBatchTime += batchTimer.ElapsedMilliseconds;
                    timer.Stop();
                    totalLockTime += timer.ElapsedMilliseconds;
                }

                totalWaitTime += timer1.ElapsedMilliseconds;
                totalTime += timer2.ElapsedMilliseconds;

            }
            catch (Exception e)
            {
                _logger.LogError("Patch JSON failed. {Message}\n {StackTrace}", e.Message, e.StackTrace);
                throw e;
            }

            _logger.LogInformation($"lock: {totalLockTime}ms, waiting: {totalWaitTime}ms, processing: {totalTime}ms, retract: {totalRetractTime}ms, assert: {totalAssertTime}ms, batch: {totalBatchTime}ms, length: {inputs.Count}");

        }

        public void PatchTriple(string storeId, JObject input)
        {

            var triplesToRetract = Enumerable.Empty<Triple>();
            var triplesToAdd = Enumerable.Empty<Triple>();

            var remove = input["remove"];
            if (remove != null && remove is JObject)
            {
                triplesToRetract = TripleConverter.FromJson((JObject)remove);
            }
            var add = input["add"];
            if (add != null && add is JObject)
            {
                triplesToAdd = TripleConverter.FromJson((JObject)add);
            }

            try
            {
                using (var op = _multiKeyLockFactory.WriteLock(storeId, triplesToAdd))
                {
                    var (data, _, _) = GetSetGraphs(storeId);
                    if (remove is JObject)
                    {
                        data.Retract(triplesToRetract);
                    }
                    if (add is JObject)
                    {
                        data.Assert(triplesToAdd);
                    }
                }

            }
            catch (Exception e)
            {
                _logger.LogError("Patch triple failed. {Message}\n {StackTrace}", e.Message, e.StackTrace);
                throw e;
            }
        }

        public void Delete(string storeId, JToken token)
        {
            JArray inputs;
            try
            {
                if (token is JObject)
                {
                    inputs = new JArray() { token };
                }
                else if (token is JArray)
                {
                    inputs = token as JArray;
                }
                else
                {
                    throw new InvalidOperationException("Invalid delete");
                }
                foreach (var input in inputs)
                {
                    if (!(input is JObject))
                    {
                        throw new InvalidOperationException("Invalid delete");
                    }
                    var triples = TripleConverter.FromJson(input as JObject);
                    using (var op = _multiKeyLockFactory.WriteLock(storeId, triples))
                    {
                        var (data, _, _) = GetSetGraphs(storeId);
                        data.Retract(triples);
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError("Delete failed. {Message}\n {StackTrace}", e.Message, e.StackTrace);
            }
        }

        public void AssertMeta(string storeId, JObject value)
        {
            var graph = TripleConverter.FromJson(value);
            using (var op = _multiKeyLockFactory.WriteLock(storeId, graph))
            {
                var (data, infer, meta) = GetSetGraphs(storeId);
                _reasoner.Spin(data, infer, meta);
            }
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

            if (!triples.Any())
            {
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
            try
            {
                var (data, _, _) = GetSetGraphs(storeId);
                ObjectQueryModel queryModel;
                try
                {
                    queryModel = query.ToObject<ObjectQueryModel>();
                }
                catch (Exception e)
                {
                    throw new StoreException(e.Message, _storeErrors.UnableToParseQuery);
                }

                var result = new ObjectQueryExecutor().Query(queryModel, data);
                dynamic response = new
                {
                    values = result.Values?.Select(x =>
                    {
                        var expanded = GraphOperator.Expand(data, x.Subject, level, expand);
                        var rspGraph = new MemoryGraph();
                        rspGraph.Assert(expanded).ToList();
                        return TripleConverter.ToJson(x.Subject, rspGraph);
                    }),
                    continuation = result.Continuation,
                    aggregates = result.Aggregates
                };

                return JObject.FromObject(response);
            }
            catch (Exception e)
            {
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
    }
}
