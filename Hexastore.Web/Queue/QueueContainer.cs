using System;
using System.Linq;
using System.Threading.Tasks;
using Hexastore.Web.EventHubs;
using Microsoft.Extensions.Logging;

namespace Hexastore.Web.Queue
{
    public class QueueContainer : IQueueContainer, IDisposable
    {
        private readonly EventReceiver _eventReceiver;
        private readonly QueueWriter[] _queueWriters;
        private readonly ILogger<QueueContainer> _logger;
        private readonly int _count = 32;
        private bool _running = true;

        public QueueContainer(EventReceiver eventReceiver, ILogger<QueueContainer> logger)
        {
            _logger = logger;
            _eventReceiver = eventReceiver;
            _queueWriters = new QueueWriter[_count];
            for(int i=0; i< _count; i++) {
                _queueWriters[i] = new QueueWriter(eventReceiver, logger);
            }
            _ = _eventReceiver.LogCount();
            _ = LogQueueLength();
        }

        public void Dispose()
        {
            _running = false;
            foreach(var q in _queueWriters) {
                q.Dispose();
            }
        }

        public void Send(StoreEvent storeEvent)
        {
            var partitionId = Math.Abs(storeEvent.PartitionId.GetHashCode() % _count);
            _queueWriters[partitionId].Send(storeEvent);
        }

        private async Task LogQueueLength()
        {
            while (_running) {
                await Task.Delay(10000);
                _logger.LogInformation($"{DateTime.Now.ToString("hh':'mm':'ss")} Queue Length: {_queueWriters.Sum(x=> x.Length)}");
            }
        }
    }
}
