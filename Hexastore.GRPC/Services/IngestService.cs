namespace Hexastore.GRPC.Services
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Grpc.Core;
    using Hexastore.Graph;
    using Hexastore.Processor;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json.Linq;

    public class IngestService : Ingest.IngestBase
    {
        private readonly ILogger<IngestService> _logger;
        private readonly IStoreProcessor _storeProcessor;

        public IngestService(ILogger<IngestService> logger, IStoreProcessor storeProcessor)
        {
            _logger = logger;
            _storeProcessor = storeProcessor;
        }

        public override Task<AssertResponse> Patch(AssertRequest request, ServerCallContext context)
        {
            return ProcessRequest(request, context, (triples) => { _storeProcessor.Patch(request.StoreId, triples); });
        }

        public override Task<AssertResponse> Assert(AssertRequest request, ServerCallContext context)
        {
            return ProcessRequest(request, context, (triples) => { _storeProcessor.Assert(request.StoreId, triples); });
        }

        public Task<AssertResponse> ProcessRequest(AssertRequest request, ServerCallContext context, Action<IEnumerable<Triple>> func)
        {
            try {
                var batch = new List<Triple>();
                int count = 0;

                foreach (var item in request.Triples) {
                    if (string.IsNullOrEmpty(item.Object)) {
                        JValue token = GetValue(item);
                        var tripleObject = new TripleObject(token, false, item.ArrayIndex);
                        batch.Add(new Triple(item.Subject, item.Predicate, tripleObject));
                        count++;
                    } else {
                        batch.Add(new Triple(item.Subject, item.Predicate, item.Object));
                        count++;
                    }
                }

                func(batch);

                return Task.FromResult(new AssertResponse {
                    Message = "Done",
                    Count = count
                });
            } catch (Exception e) {
                _logger.LogError(e, "Cannot assert to store");
                throw;
            }
        }

        private JValue GetValue(TripleMessage item)
        {
            switch (item.Type) {
                case TripleMessage.Types.ValueType.Int:
                    return new JValue(item.IntValue);
                case TripleMessage.Types.ValueType.String:
                    return new JValue(item.StringValue);
                case TripleMessage.Types.ValueType.Double:
                    return new JValue(item.DoubleValue);
                case TripleMessage.Types.ValueType.Bool:
                    return new JValue(item.BoolValue);
                default:
                    throw new ArgumentException("Unknown Value Type");

            }
        }
    }
}
