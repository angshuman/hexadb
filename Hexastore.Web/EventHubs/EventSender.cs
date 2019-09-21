using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.EventHubs
{
    public class EventSender : IDisposable
    {
        private readonly EventReceiver _storeReceiver;
        private Checkpoint _checkpoint;
        private readonly string _eventHubConnectionString;
        private readonly string _eventHubPartition;
        private readonly EventHubClient _eventHubClient;
        private readonly bool _active;

        private PartitionReceiver _receiver;

        public int MaxBatchSize
        {
            get
            {
                return 1000;
            }
            set
            {
            }
        }

        public EventSender(EventReceiver receiver, Checkpoint checkpoint)
        {
            _storeReceiver = receiver;
            _checkpoint = checkpoint;
            _eventHubConnectionString = Environment.GetEnvironmentVariable("HEXASTORE_EVENTHUB_KEY");
            _eventHubPartition = Environment.GetEnvironmentVariable("HEXASTORE_EVENTHUB_PARTITION");
            if (!string.IsNullOrEmpty(_eventHubConnectionString)) {
                _active = true;
                _eventHubClient = EventHubClient.CreateFromConnectionString(_eventHubConnectionString);
                var cp = _checkpoint.Get(Constants.EventHubCheckpoint);
                _receiver = _eventHubClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, _eventHubPartition, EventPosition.FromOffset(cp ?? "-1"));
                _receiver.SetReceiveHandler(_storeReceiver);
                _ = _storeReceiver.LogCount();
            }
        }

        public async Task SendMessage(JObject obj)
        {
            if (!_active) {
                // pass through
                await _storeReceiver.ProcessEventsAsync(obj);
                return;
            }

            try {
                var content = obj.ToString();
                var bytes = Encoding.UTF8.GetBytes(content);
                await _eventHubClient.SendAsync(new EventData(bytes), _eventHubPartition);
            } catch (Exception e) {
                Console.WriteLine(e);
            }
        }

        public void Dispose()
        {
            if (_eventHubClient != null) {
                _eventHubClient.Close();
            }
        }
    }
}
