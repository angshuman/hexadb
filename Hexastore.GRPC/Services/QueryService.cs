namespace Hexastore.GRPC.Services
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Google.Protobuf.Collections;
    using Grpc.Core;
    using Hexastore.Graph;
    using Hexastore.Processor;
    using Hexastore.Query;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json.Linq;

    public class QueryService : Query.QueryBase
    {
        private readonly ILogger<QueryService> _logger;
        private readonly IStoreProcessor _storeProcessor;

        public QueryService(ILogger<QueryService> logger, IStoreProcessor storeProcessor)
        {
            _logger = logger;
            _storeProcessor = storeProcessor;
        }

        public override async Task SelectAll(QueryRequest request, IServerStreamWriter<TripleMessage> responseStream, ServerCallContext context)
        {
            try {
                var all = _storeProcessor.SelectAll(request.StoreId);
                foreach(var item in all) {
                    await responseStream.WriteAsync(item.ConvertToTripleMessage());
                }

            } catch (Exception e) {
                _logger.LogError("Query Error", e);
                throw;
            }
        }

        public override Task<QueryResponse> Query(QueryRequest request, ServerCallContext context)
        {
            try {
                ObjectQueryModel query = ConvertToModel(request.Query);
                var response = _storeProcessor.QueryTriples(request.StoreId, JObject.FromObject(query), request.Expand.ToArray(), request.Levels);
                return Task.FromResult(ToQueryResponse(response));
            } catch (Exception e) {
                _logger.LogError("Query Error", e);
                throw;
            }
        }

        private ObjectQueryModel ConvertToModel(GRPC.ObjectQuery request)
        {
            var model = new ObjectQueryModel();

            model.Id = string.IsNullOrEmpty(request.Id) ? null : request.Id;
            model.Filter = ConvertFilter(request.Filter);
            model.HasSubject = ConvertLinkQuery(request.HasSubject);
            model.HasObject = ConvertLinkQuery(request.HasObject);
            model.PageSize = request.PageSize;
            model.Continuation = request.Continuation?.ConvertToTriple();

            // todo: aggregates

            return model;
        }

        private LinkQuery[] ConvertLinkQuery(RepeatedField<GRPC.LinkQuery> requests)
        {
            var response = new List<LinkQuery>();
            foreach (var request in requests) {
                var linkQuery = new LinkQuery();
                linkQuery.Target = ConvertToModel(request.Target);
                linkQuery.Path = request.Path;
                linkQuery.Level = request.Level;
                response.Add(linkQuery);
            }

            return response.ToArray();
        }

        private IDictionary<string, QueryUnit> ConvertFilter(MapField<string, GRPC.QueryUnit> filter)
        {
            var response = new Dictionary<string, QueryUnit>();
            foreach (var item in filter) {
                response[item.Key] = ConvertQueryUnit(item.Value);
            }
            return response;
        }

        private QueryUnit ConvertQueryUnit(GRPC.QueryUnit value)
        {
            var response = new QueryUnit {
                Operator = value.Operator
            };

            switch (value.Type) {
                case GRPC.QueryUnit.Types.ValueType.Bool:
                    response.Value = value.BoolValue;
                    break;
                case GRPC.QueryUnit.Types.ValueType.String:
                    response.Value = value.StringValue;
                    break;
                case GRPC.QueryUnit.Types.ValueType.Int:
                    response.Value = value.IntValue;
                    break;
                case GRPC.QueryUnit.Types.ValueType.Double:
                    response.Value = value.DoubleValue;
                    break;
            }

            return response;
        }

        private QueryResponse ToQueryResponse(ObjectQueryResponse oqr)
        {
            var response = new QueryResponse();
            response.Triples.AddRange(oqr.Values.Select(x => x.ConvertToTripleMessage()));

            if (oqr.Continuation != null) {
                response.Continuation = new Triple(oqr.Continuation.S, oqr.Continuation.P, new TripleObject(oqr.Continuation.O, oqr.Continuation.IsId, oqr.Continuation.Index)).ConvertToTripleMessage();
            }

            return response;
        }
    }
}
