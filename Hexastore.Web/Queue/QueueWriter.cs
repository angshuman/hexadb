using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Hexastore.Errors;
using Hexastore.Web.EventHubs;
using Microsoft.Extensions.Logging;

namespace Hexastore.Web.Queue
{
    public class QueueWriter : IDisposable
    {
        private readonly BlockingCollection<StoreEvent> _queue;
        private readonly EventReceiver _eventReceiver;
        private readonly StoreError _storeError;
        private readonly ILogger _logger;
        private readonly Task _task;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly int DefaultMaxQueueSize = 4096;
        private readonly int _maxQueueSize;

        public QueueWriter(EventReceiver eventReceiver, ILogger logger, StoreError storeError, int maxQueueSize = 0)
        {
            _eventReceiver = eventReceiver;
            _storeError = storeError;
            _queue = new BlockingCollection<StoreEvent>();
            _logger = logger;
            _maxQueueSize = maxQueueSize > 0 ? maxQueueSize : DefaultMaxQueueSize;
            _task = StartReader();
        }

        public int Length => _queue.Count;

        public void Dispose()
        {
            _cts.Cancel();
            _queue.Dispose();
        }

        public void Send(StoreEvent storeEvent)
        {
            if (_queue.Count > _maxQueueSize) {
                throw _storeError.MaxQueueSize;
            }
            _queue.Add(storeEvent);
        }

        private Task StartReader()
        {
             return Task.Run(() => {
                while (!_cts.IsCancellationRequested) {
                    try {
                        var storeEvent = _queue.Take(_cts.Token);
                        _eventReceiver.ProcessEventsAsync(storeEvent);
                    } catch(Exception e) {
                        _logger.LogError("Error in reading from queue {e}", e);
                    }
                }
            });
        }
    }
}
