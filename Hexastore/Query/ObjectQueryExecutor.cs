using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using Hexastore.Errors;
using Hexastore.Graph;
using Hexastore.Web.Errors;
using Newtonsoft.Json.Linq;

namespace Hexastore.Query
{
    public class ObjectQueryExecutor
    {
        private readonly StoreError _storeErrors;
        private static readonly char[] LinkDelimiterArray = Constants.PathDelimeter.ToCharArray();

        public ObjectQueryExecutor()
        {
            _storeErrors = new StoreError();
        }
        public ObjectQueryResponse Query(ObjectQueryModel query, IStoreGraph graph)
        {
            query.PageSize = query.PageSize != 0 ? query.PageSize : Constants.DefaultPageSize;
            if (query.Id != null) {
                var item = graph.S(query.Id).FirstOrDefault();
                return new ObjectQueryResponse {
                    Values = item != null ? new Triple[] { item } : new Triple[0],
                    Continuation = null
                };
            }

            if (query.Filter == null) {
                throw _storeErrors.AtLeastOneFilter;
            }

            var firstFilter = query.Filter.FirstOrDefault();
            if (firstFilter.Key == null) {
                throw _storeErrors.AtLeastOneFilter;
            }

            var cTriple = query.Continuation != null
                ? new Triple(
                    query.Continuation.S,
                    query.Continuation.P,
                    new TripleObject(query.Continuation.O, query.Continuation.IsId, query.Continuation.Index))
                : null;

            var rsp = CreateConstraint(graph, firstFilter.Key, firstFilter.Value, cTriple);
            foreach (var filter in query.Filter.Skip(1)) {
                rsp = ApplyConstraint(rsp, graph, filter.Key, filter.Value);
            }

            if (query.HasObject != null) {
                foreach (var obj in query.HasObject) {
                    rsp = ApplyOutgoing(rsp, graph, obj);
                }
            }

            if (query.HasSubject != null) {
                foreach (var sub in query.HasSubject) {
                    rsp = ApplyIncoming(rsp, graph, sub);
                }
            }

            if (query.Aggregates == null || query.Aggregates.Length == 0) {
                var responseTriples = rsp.Take(query.PageSize).ToArray();
                var cont = responseTriples.Length < query.PageSize ? null : responseTriples.LastOrDefault();
                var queryResponse = new ObjectQueryResponse {
                    Values = responseTriples,
                    Continuation = cont != null
                    ? new Continuation {
                        S = cont.Subject,
                        P = cont.Predicate,
                        O = cont.Object.ToTypedJSON(),
                        IsId = cont.Object.IsID,
                        Index = cont.Object.Index
                    }
                    : null
                };
                return queryResponse;
            }

            // process aggregates
            return ApplyAggregates(rsp, query.Aggregates);
        }

        private ObjectQueryResponse ApplyAggregates(IEnumerable<Triple> rsp, AggregateQuery[] aggregates)
        {
            var responses = new List<object>();
            foreach (var aggregate in aggregates) {
                switch (aggregate.Type) {
                    case AggregateType.Count:
                        var count = rsp.Count();
                        responses.Add(count);
                        break;
                    default:
                        throw new InvalidOperationException("unknown aggregate");
                }
            }
            return new ObjectQueryResponse {
                Aggregates = new object[]
                {
                    responses
                }
            };
        }

        private IEnumerable<Triple> ApplyConstraint(IEnumerable<Triple> rsp, IGraph graph, string key, QueryUnit value)
        {
            var input = new JValue(value.Value);
            switch (value.Operator) {
                case "eq":
                    return rsp.Where(x => graph.Exists(x.Subject, key, TripleObject.FromData(value.Value.ToString())));
                case "gt":
                case "ge":
                case "lt":
                case "le":
                case "contains":
                    return rsp.Where((x) => {
                        var t = graph.SP(x.Subject, key).Any(Comparator(value));
                        return t;
                    });
                default:
                    throw _storeErrors.UnknownComparator;
            }
        }

        private IEnumerable<Triple> CreateConstraint(IStoreGraph graph, string key, QueryUnit value, Triple continuation)
        {
            var input = new JValue(value.Value);
            switch (value.Operator) {
                case "eq":
                    return graph.PO(key, TripleObject.FromData(value.Value.ToString()), continuation);
                case "gt":
                case "ge":
                case "lt":
                case "le":
                case "contains":
                    return graph.P(key, continuation).Where(Comparator(value));
                default:
                    throw _storeErrors.UnknownComparator;
            }
        }

        private Func<Triple, bool> Comparator(QueryUnit value)
        {
            var input = new JValue(value.Value);
            switch (value.Operator) {
                case "gt":
                    return (Triple x) => {
                        var jValue = x.Object.ToTypedJSON();
                        return (jValue.CompareTo(input) > 0);
                    };
                case "ge":
                    return (Triple x) => {
                        var jValue = x.Object.ToTypedJSON();
                        return (jValue.CompareTo(input) >= 0);
                    };
                case "lt":
                    return (Triple x) => {
                        var jValue = x.Object.ToTypedJSON();
                        return (jValue.CompareTo(input) < 0);
                    };
                case "le":
                    return (Triple x) => {
                        var jValue = x.Object.ToTypedJSON();
                        return (jValue.CompareTo(input) <= 0);
                    };
                case "contains":
                    return (Triple x) => {
                        var jValue = x.Object.ToValue();
                        return jValue.Contains(value.Value.ToString());
                    };
                default:
                    throw _storeErrors.UnknownComparator;
            }
        }

        private IEnumerable<Triple> ApplyOutgoing(IEnumerable<Triple> source, IStoreGraph graph, LinkQuery link)
        {
            if (link == null) {
                return source;
            }

            if (string.IsNullOrEmpty(link.Path)) {
                throw _storeErrors.PathEmpty;
            }

            var paths = link.Path.Split(LinkDelimiterArray);

            var matched = source.Where(x => {
                IEnumerable<string> targets;
                var segments = new Queue<string>(paths);
                if (link.Level == 0) {
                    targets = GetByLink(graph, new string[] { x.Subject }, segments, (gx, sx, seg) => GetSubjectLink(gx, sx, seg));
                } else {
                    targets = GetByLevel(graph, new string[] { x.Subject }, link.Level, true);
                }
                // todo: use DP to remember nodes that have matched before
                return targets.Any(t => SubjectMatch(t, graph, link.Target));
            });
            return matched;
        }

        private IEnumerable<Triple> ApplyIncoming(IEnumerable<Triple> source, IStoreGraph graph, LinkQuery link)
        {
            if (link == null) {
                return source;
            }

            if (string.IsNullOrEmpty(link.Path)) {
                throw _storeErrors.PathEmpty;
            }

            var paths = link.Path.Split(LinkDelimiterArray).Reverse();

            var matched = source.Where(x => {
                IEnumerable<string> targets;

                var segments = new Queue<string>(paths);
                if (link.Level == 0) {
                    targets = GetByLink(graph, new string[] { x.Subject }, segments, (gx, sx, seg) => GetObjectLink(gx, sx, seg));
                } else {
                    targets = GetByLevel(graph, new string[] { x.Subject }, link.Level, false);
                }
                return targets.Any(t => SubjectMatch(t, graph, link.Target));
            });
            return matched;
        }

        private bool SubjectMatch(string t, IStoreGraph graph, ObjectQueryModel target)
        {
            if (target.Id != null) {
                return t == target.Id;
            }
            var result = true;
            foreach (var filter in target.Filter) {
                switch (filter.Value.Operator) {
                    case "eq":
                        result &= graph.Exists(t, filter.Key, TripleObject.FromData(filter.Value.Value.ToString()));
                        break;
                    case "gt":
                    case "ge":
                    case "lt":
                    case "le":
                    case "contains":
                        var match = graph.SP(t, filter.Key).Any(Comparator(filter.Value));
                        result &= match;
                        break;
                    default:
                        throw _storeErrors.UnknownComparator;
                }
            }
            return result;
        }

        private IEnumerable<string> GetByLink(IStoreGraph graph, IEnumerable<string> sources, Queue<string> segments, Func<IStoreGraph, string, string, IEnumerable<string>> f)
        {
            if (segments.Count == 0) {
                return sources;
            } else {
                var segment = segments.Dequeue();
                IEnumerable<string> next = new List<string>();
                foreach (var source in sources) {
                    next = next.Concat(f(graph, source, segment));
                }
                return GetByLink(graph, next, segments, f);
            }
        }

        private IEnumerable<string> GetByLevel(IStoreGraph graph, IEnumerable<string> sources, int level, bool isOutgoing)
        {
            if (level == 0) {
                return Enumerable.Empty<string>();
            }
            IEnumerable<string> next = new List<string>();
            foreach (var source in sources) {
                var items = isOutgoing
                    ? graph.S(source).Where(x => x.Object.IsID).Select(y => y.Object.Id).Distinct()
                    : graph.O(source).Select(y => y.Subject).Distinct();
                var targets = GetByLevel(graph, items, level - 1, isOutgoing);
                next = next.Concat(targets);
            }
            return sources.Concat(next).Distinct();
        }

        private IEnumerable<string> GetSubjectLink(IStoreGraph graph, string source, string segment)
        {
            if (string.Compare(segment, Constants.AnyPath, StringComparison.Ordinal) == 0) {
                return graph.S(source).Where(x => x.Object.IsID).Select(x => x.Object.ToValue());
            } else {
                return graph.SP(source, segment).Where(x => x.Object.IsID).Select(x => x.Object.ToValue());
            }
        }

        private IEnumerable<string> GetObjectLink(IStoreGraph graph, string source, string segment)
        {
            if (string.Compare(segment, Constants.AnyPath, StringComparison.Ordinal) == 0) {
                return graph.O(source).Select(x => x.Subject);
            } else {
                return graph.PO(segment, source).Select(x => x.Subject);
            }
        }
    }
}
