using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Hexastore.Web.EventHubs;
using Microsoft.Extensions.Logging;

namespace Hexastore.Web.Queue
{
    public class QueueWriter : IDisposable
    {
        private readonly BlockingCollection<StoreEvent> _queue;
        private readonly EventReceiver _eventReceiver;
        private readonly ILogger _logger;
        private readonly Task _task;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        public QueueWriter(EventReceiver eventReceiver, ILogger logger)
        {
            _queue = new BlockingCollection<StoreEvent>();
            _eventReceiver = eventReceiver;
            _logger = logger;
            _task = StartReader();
            _task.Start();
        }

        public int Length => _queue.Count;

        public void Dispose()
        {
            _cts.Cancel();
            _queue.Dispose();
        }

        public void Send(StoreEvent storeEvent)
        {
            _queue.Add(storeEvent);
        }

        private Task StartReader()
        {
            return new Task(() => {
                while (!_cts.IsCancellationRequested) {
                    try {
                        var storeEvent = _queue.Take();
                        _eventReceiver.ProcessEventsAsync(storeEvent);
                    } catch(Exception e) {
                        _logger.LogError("Error in reading from queue {e}", e);
                    }
                }
            });
        }
    }
}
