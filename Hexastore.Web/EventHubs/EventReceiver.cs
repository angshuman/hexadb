using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Hexastore.Processor;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.EventHubs
{

    public class EventReceiver : IPartitionReceiveHandler, IDisposable
    {
        private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> _completions;
        private readonly ILogger<EventReceiver> _logger;
        private readonly Checkpoint _checkpoint;
        private readonly IStoreProcessor _storeProcessor;
        private readonly StoreConfig _storeConfig;
        private bool _running = true;

        private int _eventCount;

        public EventReceiver(IStoreProcessor storeProcessor, Checkpoint checkpoint, StoreConfig storeConfig, ILogger<EventReceiver> logger)
        {
            _completions = new ConcurrentDictionary<string, TaskCompletionSource<bool>>();
            _logger = logger;
            _checkpoint = checkpoint;
            _storeProcessor = storeProcessor;
            _storeConfig = storeConfig;

            _eventCount = 0;
        }

        public int MaxBatchSize
        {
            get {
                return 1000;
            }
            set {
            }
        }

        public async Task LogCount()
        {
            var lastCount = 0;
            while (_running) {
                await Task.Delay(10000);
                _logger.LogInformation($"{DateTime.Now.ToString("hh':'mm':'ss")} Events: {_eventCount} Diff: {_eventCount - lastCount}");
                lastCount = _eventCount;
            }
        }

        public async Task ProcessEventsAsync(IEnumerable<EventData> events)
        {
            if (events == null) {
                return;
            }

            foreach (var e in events) {
                _eventCount++;
                var content = Encoding.UTF8.GetString(e.Body);
                var partitionId = e.SystemProperties["x-opt-partition-key"].ToString();
                var offset = e.SystemProperties["x-opt-offset"].ToString();
                StoreEvent storeEvent;
                try {
                    storeEvent = StoreEvent.FromString(content);
                } catch (Exception exception) {
                    _logger.LogError(exception, "Unable to process event {offset} {partition} {}", offset, partitionId);
                    continue;
                }

                await ProcessEventsAsync(storeEvent);
                _checkpoint.Write($"{Constants.EventHubCheckpoint}.{_storeConfig.EventHubName}.{partitionId}", offset);
            }

            return;
        }

        public Task ProcessEventsAsync(StoreEvent storeEvent)
        {
            var storeId = storeEvent.StoreId;

            string operation = storeEvent.Operation;
            var opId = storeEvent.OperationId;
            var strict = storeEvent.Strict;

            TaskCompletionSource<bool> tc = null;
            if (opId != null) {
                _completions.TryGetValue(opId, out tc);
            }

            try {
                var data = storeEvent.Data;
                switch (operation) {
                    case EventType.POST:
                        _storeProcessor.Assert(storeId, data, strict);
                        break;
                    case EventType.PATCH_JSON:
                        _storeProcessor.PatchJson(storeId, data);
                        break;
                    case EventType.PATCH_TRIPLE:
                        var patch = (JObject)storeEvent.Data;
                        _storeProcessor.PatchTriple(storeId, patch);
                        break;
                    case EventType.DELETE:
                        _storeProcessor.Delete(storeId, data);
                        break;
                    case EventType.CREATETWIN:
                        _storeProcessor.CreateTwin(storeId, data);
                        break;
                    case EventType.CREATERELATIONSHIP:
                        _storeProcessor.CreateRelationship(storeId, data);
                        break;
                    default:
                        throw new InvalidOperationException($"Unknown operation {operation}");
                }
                tc?.SetResult(true);
            } catch (Exception exception) {
                tc?.SetException(exception);
            } finally {
                if (tc != null) {
                    _completions.Remove(opId, out _);
                }
            }
            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(Exception error)
        {
            Console.WriteLine(JsonConvert.SerializeObject(error));
            return Task.CompletedTask;
        }

        public void SetCompletion(string guid, TaskCompletionSource<bool> tc)
        {
            _completions.TryAdd(guid, tc);
        }

        public void Dispose()
        {
            _running = false;
        }
    }
}
