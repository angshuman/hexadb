using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Hexastore.Errors;
using Hexastore.Processor;
using Hexastore.Web.EventHubs;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
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
            : base(receiver, processor, storeError, logger)
        {
            _storeProcessor = storeProcessor;
            _logger = logger;
        }

        [HttpGet("health")]
        public IActionResult Get()
        {
            _logger.LogInformation(LoggingEvents.ControllerHealth, "health");
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
            _logger.LogInformation(LoggingEvents.ControllerGetSubject, "GET: store {store} subject {subject}", storeId, subject);
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
            _logger.LogInformation(LoggingEvents.ControllerQuery, "QUERY: store {store}", storeId);
            try {
                var (_, expand, level, _) = GetParams();
                var rsp = _storeProcessor.Query(storeId, query, expand, level);
                return Ok(rsp);
            } catch (Exception e) {
                return HandleException(e);
            }
        }

        [HttpGet("{storeId}/predicates")]
        public IActionResult Predicates(string storeId)
        {
            _logger.LogInformation(LoggingEvents.ControllerPredicates, $"PREDICATES: store {storeId}");
            try
            {
                var rsp = _storeProcessor.GetPredicates(storeId);
                return Ok(rsp);
            } catch (Exception e)
            {
                return HandleException(e);
            }
        }

        [HttpPost("{storeId}/ingest")]
        public async Task<IActionResult> Ingest(string storeId, [FromBody]JObject body)
        {
            _logger.LogInformation(LoggingEvents.ControllerIngest, "INGEST: store {store}", storeId);
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
                            var e = new StoreEvent
                            {
                                Operation = EventType.POST,
                                Strict = true,
                                Data = batch.ToString(Formatting.None)
                            };
                            await SendEvent(storeId, e);
                            _logger.LogInformation("Batch ingestion", batch.Count);
                            batch = new JArray();
                        }
                    }

                    if (batch.Count > 0) {
                        var e = new StoreEvent
                        {
                            Operation = EventType.POST,
                            Strict = true,
                            Data = batch.ToString(Formatting.None)
                        };
                        await SendEvent(storeId, e);
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
            _logger.LogInformation(LoggingEvents.ControllerPost, "POST: store {store}", storeId);
            try {
                var (_, _, _, strict) = GetParams();
                var e = new StoreEvent
                {
                    Operation = EventType.POST,
                    Strict = strict,
                    Data = data.ToString(Formatting.None)
                };
                await SendEvent(storeId, e);
                return Accepted();
            } catch (Exception e) {
                return HandleException(e);
            }
        }

        [HttpPatch("{storeId}/json")]
        public async Task<IActionResult> PatchJson(string storeId, [FromBody]JToken data)
        {
            _logger.LogInformation(LoggingEvents.ControllerPatchJson, "PATCH JSON: store {storeId}", storeId);
            try {
                var e = new StoreEvent
                {
                    Operation = EventType.PATCH_JSON,
                    Data = data.ToString(Formatting.None)
                };
                await SendEvent(storeId, e);
                return Accepted();
            } catch (Exception e) {
                return HandleException(e);
            }
        }

        [HttpPatch("{storeId}/batch/json")]
        public async Task<IActionResult> BatchPatchJson(string storeId, [FromBody]IEnumerable<JToken> dataPatches)
        {
            _logger.LogInformation(LoggingEvents.ControllerPatchJson, "BATCH PATCH JSON: store {storeId}", storeId);
            try
            {
                foreach(var patch in dataPatches)
                {
                    var e = new StoreEvent
                    {
                        Operation = EventType.PATCH_JSON,
                        Data = patch.ToString(Formatting.None)
                    };
                    await SendEvent(storeId, e);
                }
                return Accepted();
            }
            catch (Exception e)
            {
                return HandleException(e);
            }
        }

        [HttpPatch("{storeId}/triple")]
        public async Task<IActionResult> PatchTriple(string storeId, [FromBody]JObject data)
        {
            _logger.LogInformation(LoggingEvents.ControllerPatchTriple, "PATCH TRIPLE: store {store}", storeId);
            try {
                var e = new StoreEvent
                {
                    Operation = EventType.PATCH_TRIPLE,
                    Data = data.ToString(Formatting.None)
                };
                await SendEvent(storeId, e);
                return Accepted();
            } catch (Exception e) {
                return HandleException(e);
            }
        }

        [HttpDelete("{storeId}/subject")]
        public async Task<IActionResult> Delete(string storeId, [FromBody]JObject data)
        {
            _logger.LogInformation(LoggingEvents.ControllerDelete, "DELETE: store {store}", storeId);
            try {
                var e = new StoreEvent
                {
                    Operation = EventType.DELETE,
                    Data = data.ToString(Formatting.None)
                };
                await SendEvent(storeId, e);
                return Accepted();
            } catch (Exception e) {
                return HandleException(e);
            }
        }
    }
}
