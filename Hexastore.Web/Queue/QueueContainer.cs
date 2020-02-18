using System;
using Hexastore.Web.EventHubs;
using Microsoft.Extensions.Logging;

namespace Hexastore.Web.Queue
{
    public class QueueContainer : IQueueContainer
    {
        private readonly EventReceiver _eventReceiver;
        private readonly QueueWriter[] _queueWriters;
        private int _count = 32;

        public QueueContainer(EventReceiver eventReceiver, ILogger<QueueContainer> logger)
        {
            _eventReceiver = eventReceiver;
            _queueWriters = new QueueWriter[_count];
            for(int i=0; i< _count; i++) {
                _queueWriters[i] = new QueueWriter(eventReceiver, logger);
            }
            _ = _eventReceiver.LogCount();
        }

        public void Send(StoreEvent storeEvent)
        {
            var partitionId = Math.Abs(storeEvent.PartitionId.GetHashCode() % _count);
            _queueWriters[partitionId].Send(storeEvent);
        }
    }
}
