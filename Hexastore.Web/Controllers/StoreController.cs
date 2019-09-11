using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Hexastore.Processor;
using Hexastore.Web.EventHubs;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StoreController : StoreControllerBase
    {
        private readonly IStoreProcesor storeProcessor;

        public StoreController(IStoreProcesor storeProcessor, EventReceiver receiver, EventSender processor) : base(receiver, processor)
        {
            this.storeProcessor = storeProcessor;
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
                var rsp = storeProcessor.GetSubject(storeId, subject, expand, level);
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
                var rsp = storeProcessor.Query(storeId, query, expand, level);
                return Ok(rsp);
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

        [HttpPatch("{storeId}")]
        public async Task<IActionResult> Patch(string storeId, [FromBody]JObject data)
        {
            try {
                var e = new
                {
                    operation = "PATCH",
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
