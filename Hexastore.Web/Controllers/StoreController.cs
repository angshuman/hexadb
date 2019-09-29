using System;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Hexastore.Errors;
using Hexastore.Processor;
using Hexastore.Web.EventHubs;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StoreController : StoreControllerBase
    {
        private readonly IStoreProcesor _storeProcessor;
        private readonly ILogger _logger;

        public StoreController(IStoreProcesor storeProcessor, EventReceiver receiver, EventSender processor, StoreError storeError, ILogger<StoreController> logger)
            : base(receiver, processor, storeError)
        {
            _storeProcessor = storeProcessor;
            _logger = logger;
        }

        [HttpGet("health")]
        public IActionResult Get()
        {
            var info = new
            {
                RuntimeInformation.OSDescription,
                RuntimeInformation.FrameworkDescription,
                RuntimeInformation.OSArchitecture,
                RuntimeInformation.ProcessArchitecture,
                WorkingSet = (Environment.WorkingSet / 1000_000).ToString(),
                Environment.ProcessorCount
            };
            return Ok(info);
        }

        [HttpGet("{storeId}/{subject}")]
        public IActionResult Get(string storeId, string subject)
        {
            try {
                var (_, expand, level, _) = GetParams();
                var rsp = _storeProcessor.GetSubject(storeId, subject, expand, level);
                return Ok(rsp);
            } catch (Exception e) {
                return HandleException(e);
            }
        }

        [HttpPost("{storeId}/query")]
        public IActionResult Query(string storeId, [FromBody]JObject query)
        {
            try {
                var (_, expand, level, _) = GetParams();
                var rsp = _storeProcessor.Query(storeId, query, expand, level);
                return Ok(rsp);
            } catch (Exception e) {
                return HandleException(e);
            }
        }

        [HttpPost("{storeId}/ingest")]
        public async Task<IActionResult> Ingest(string storeId, [FromBody]JObject body)
        {
            string url = body["url"]?.ToString();
            if (string.IsNullOrEmpty(url)) {
                return BadRequest();
            }
            try {
                var (_, expand, level, _) = GetParams();
                using (var client = new HttpClient()) {
                    var response = await client.GetStringAsync(url);
                    var data = JArray.Parse(response);

                    var batch = new JArray();
                    foreach (var item in data) {

                        batch.Add(item);

                        if (batch.Count == 1000) {
                            var e = new
                            {
                                operation = "POST",
                                strict = true,
                                data = batch
                            };
                            await SendEvent(storeId, JObject.FromObject(e));
                            _logger.LogInformation("Batch ingestion", batch.Count);
                            batch = new JArray();
                        }
                    }

                    if (batch.Count > 0) {
                        var e = new
                        {
                            operation = "POST",
                            strict = true,
                            data = batch
                        };
                        await SendEvent(storeId, JObject.FromObject(e));
                        _logger.LogInformation("Batch ingestion", batch.Count);

                    }
                    return Accepted();
                }

            } catch (Exception e) {
                return HandleException(e);
            }
        }

        [HttpPost("{storeId}")]
        public async Task<IActionResult> Post(string storeId, [FromBody]JToken data)
        {
            try {
                var (_, _, _, strict) = GetParams();
                var e = new
                {
                    operation = "POST",
                    strict,
                    data
                };
                await SendEvent(storeId, JObject.FromObject(e));
                return Accepted();
            } catch (Exception e) {
                return HandleException(e);
            }
        }

        [HttpPatch("{storeId}/json")]
        public async Task<IActionResult> PatchJson(string storeId, [FromBody]JToken data)
        {
            try {
                var e = new
                {
                    operation = "PATCH_JSON",
                    data
                };
                await SendEvent(storeId, JObject.FromObject(e));
                return Accepted();
            } catch (Exception e) {
                return HandleException(e);
            }
        }

        [HttpPatch("{storeId}/triple")]
        public async Task<IActionResult> PatchTriple(string storeId, [FromBody]JObject data)
        {
            try {
                var e = new
                {
                    operation = "PATCH_TRIPLE",
                    data
                };
                await SendEvent(storeId, JObject.FromObject(e));
                return Accepted();
            } catch (Exception e) {
                return HandleException(e);
            }
        }
    }
}
