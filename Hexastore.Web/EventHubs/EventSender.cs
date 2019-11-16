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
        private readonly Checkpoint _checkpoint;
        private readonly StoreConfig _storeConfig;
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

        public EventSender(EventReceiver receiver, Checkpoint checkpoint, StoreConfig storeConfig)
        {
            _storeReceiver = receiver;
            _checkpoint = checkpoint;
            _storeConfig = storeConfig;
            if (_storeConfig.ReplicationIsActive) {
                _eventHubClient = EventHubClient.CreateFromConnectionString(_storeConfig.EventHubConnectionString);
                _ = this.StartListeners();
                _ = _storeReceiver.LogCount();
            }
        }

        public async Task SendMessage(StoreEvent storeEvent)
        {
            if (!_active) {
                // pass through
                await _storeReceiver.ProcessEventsAsync(storeEvent);
                return;
            }

            try {
                storeEvent.PartitionId = storeEvent.StoreId.GetHashCode() % _storeConfig.EventHubPartitionCount;
                var content = JsonConvert.SerializeObject(storeEvent, Formatting.None);
                var bytes = Encoding.UTF8.GetBytes(content);
                await _eventHubClient.SendAsync(new EventData(bytes), storeEvent.PartitionId.ToString());
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

        private async Task StartListeners()
        {
            var ehRuntime = await _eventHubClient.GetRuntimeInformationAsync();
            foreach (var partitionId in ehRuntime.PartitionIds) {
                var cp = _checkpoint.Get($"{Constants.EventHubCheckpoint}.{_storeConfig.EventHubName}.{partitionId}");
                _receiver = _eventHubClient.CreateReceiver(PartitionReceiver.DefaultConsumerGroupName, partitionId, EventPosition.FromOffset(cp ?? "-1"));
                _receiver.SetReceiveHandler(_storeReceiver);
            }
        }
    }
}
