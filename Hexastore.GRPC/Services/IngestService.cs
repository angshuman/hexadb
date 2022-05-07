namespace Hexastore.GRPC.Services
{
    using System.Threading.Tasks;
    using Grpc.Core;
    using Microsoft.Extensions.Logging;

    public class IngestService : Ingest.IngestBase
    {
        private readonly ILogger<IngestService> _logger;

        public IngestService(ILogger<IngestService> logger)
        {
            _logger = logger;
        }

        public override Task<AssertResponse> Assert(AssertRequest request, ServerCallContext context)
        {
            return Task.FromResult(new AssertResponse {
                Message = "Done",
                Count = 1
            });
        }
    }
}
