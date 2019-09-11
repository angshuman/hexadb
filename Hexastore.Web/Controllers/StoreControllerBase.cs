using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Hexastore.Web.Errors;
using Hexastore.Web.EventHubs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.Controllers
{
    public class StoreControllerBase : ControllerBase
    {
        private readonly EventReceiver _receiver;
        private readonly EventSender _eventProcessor;

        public StoreControllerBase(EventReceiver receiver, EventSender eventProcessor)
        {
            _receiver = receiver;
            _eventProcessor = eventProcessor;
        }

        protected (string[], string[], int, bool) GetParams()
        {
            var expandFilter = Request.HttpContext.Request.Query[Constants.ExpandFilter].ToString();
            string[] expand;
            if (string.IsNullOrEmpty(expandFilter)) {
                expand = null;
            } else {
                expand = expandFilter.Split(",");
            }

            var type = Request.HttpContext.Request.Query[Constants.TypeFilter].ToString().Split(",");

            if (!int.TryParse(Request.HttpContext.Request.Query[Constants.LevelFilter].ToString(), out int level)) {
                level = 1;
            }


            if (!bool.TryParse(Request.HttpContext.Request.Query[Constants.StrictFilter].ToString(), out bool strict)) {
                strict = false;
            }
            return (type, expand, level, strict);
        }

        protected (int?, int?) GetSkip()
        {
            var skipText = Request.HttpContext.Request.Query[Constants.Skip].ToString();
            var takeText = Request.HttpContext.Request.Query[Constants.Take].ToString();

            int? skip = !string.IsNullOrWhiteSpace(skipText) ? int.Parse(skipText) : (int?)null;
            int? take = !string.IsNullOrWhiteSpace(takeText) ? int.Parse(takeText) : (int?)null;

            return (skip, take);
        }

        protected Task SendEvent(string storeId, JObject payload)
        {
            var guid = Guid.NewGuid().ToString();
            payload["operationId"] = guid;
            payload["storeId"] = storeId;

            var tc = new TaskCompletionSource<bool>();
            _receiver.SetCompletion(guid, tc);
            _ = _eventProcessor.SendMessage(payload);
            return tc.Task;
        }

        protected IActionResult HandleException(Exception e)
        {
            if (e is StoreException) {
                return new ErrorActionResult(e as StoreException);
            } else {
                return new ErrorActionResult(new StoreException(e.Message, "500.001"));
            }
        }

        public class ErrorActionResult : IActionResult
        {
            private readonly StoreException _e;

            public ErrorActionResult(StoreException e)
            {
                _e = e;
            }

            public async Task ExecuteResultAsync(ActionContext context)
            {
                int.TryParse(_e.ErrorCode.Split(".").First(), out var status);

                var objectResult = new ObjectResult(_e)
                {
                    StatusCode = status == 0
                        ? StatusCodes.Status500InternalServerError
                        : status,
                    Value = new { message = _e.Message, code = _e.ErrorCode }
                };

                await objectResult.ExecuteResultAsync(context);
            }
        }
    }
}
