using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Hexastore.Errors;
using Hexastore.Web.Errors;
using Hexastore.Web.EventHubs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.Controllers
{
    public class StoreControllerBase : ControllerBase
    {
        private readonly EventReceiver _receiver;
        private readonly EventSender _eventProcessor;
        private readonly StoreError _storeError;
        private readonly ILogger _logger;

        public StoreControllerBase(EventReceiver receiver, EventSender eventProcessor, StoreError storeError, ILogger<StoreControllerBase> logger)
        {
            _receiver = receiver;
            _eventProcessor = eventProcessor;
            _storeError = storeError;
            _logger = logger;
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

        protected async Task SendEvent(string storeId, StoreEvent payload)
        {
            // todo: resolve the promise when all messages are roundtripped from EH
            payload.StoreId = storeId;
            await _eventProcessor.SendMessage(payload);
        }

        protected Task SendEvents(string storeId, IEnumerable<StoreEvent> payloads)
        {
            var tc = new TaskCompletionSource<bool>();
            var storeEvents = payloads.ToList();
            foreach (var payload in storeEvents)
            {
                var guid = Guid.NewGuid().ToString();
                payload.OperationId = guid;
                payload.StoreId = storeId;
                _receiver.SetCompletion(guid, tc);
            }
           
            _ = _eventProcessor.SendMessages(storeEvents);
            return tc.Task;
        }


        protected IActionResult HandleException(Exception e)
        {
            _logger.LogError(LoggingEvents.ControllerError, e, e.Message);
            if (e is StoreException) {
                return new ErrorActionResult(e as StoreException);
            } else {
                return new ErrorActionResult(new StoreException(e.Message, _storeError.Unhandled));
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
