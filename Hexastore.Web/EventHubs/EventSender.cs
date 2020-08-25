using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Hexastore.Web.Queue;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.EventHubs
{
    public class EventSender : IDisposable
    {
        private readonly EventReceiver _storeReceiver;
        private readonly IQueueContainer _queueContainer;
        private readonly Checkpoint _checkpoint;
        private readonly StoreConfig _storeConfig;
        private readonly EventHubClient _eventHubClient;
        private readonly bool _active;

        private PartitionReceiver _receiver;

        public int MaxBatchSize
        {
            get {
                return 1000;
            }
            set {
            }
        }

        public EventSender(IQueueContainer queueContainer, EventReceiver storeReceiver, Checkpoint checkpoint, StoreConfig storeConfig)
        {
            _storeReceiver = storeReceiver;
            _queueContainer = queueContainer;
            _checkpoint = checkpoint;
            _storeConfig = storeConfig;
            if (_storeConfig.ReplicationIsActive) {
                _eventHubClient = EventHubClient.CreateFromConnectionString(_storeConfig.EventHubConnectionString);
                _ = StartListeners();
                _active = true;
            }
        }

        public async Task SendMessage(StoreEvent storeEvent)
        {
            if (!_active) {
                // pass through
                _queueContainer.Send(storeEvent);
                return;
            }

            try {
                int ehPartitionId;
                if (!string.IsNullOrEmpty(storeEvent.PartitionId)) {
                    ehPartitionId = storeEvent.PartitionId.GetHashCode() % _storeConfig.EventHubPartitionCount;
                } else {
                    ehPartitionId = storeEvent.StoreId.GetHashCode() % _storeConfig.EventHubPartitionCount;
                }
                var content = JsonConvert.SerializeObject(storeEvent, Formatting.None);
                var bytes = Encoding.UTF8.GetBytes(content);
                await _eventHubClient.SendAsync(new EventData(bytes), ehPartitionId.ToString());
            } catch (Exception e) {
                Console.WriteLine(e);
            }
        }

        public async Task SendMessages(IEnumerable<StoreEvent> storeEvents)
        {
            if (!_active)
            {
                // pass through
                foreach(var storeEvent in storeEvents)
                {
                    await _storeReceiver.ProcessEventsAsync(storeEvent);
                }
                return;
            }

            try
            {
                var partitionKey = string.Empty;
                var eventDatas = new List<EventData>();
                foreach (var storeEvent in storeEvents)
                {
                    storeEvent.PartitionId = storeEvent.StoreId.GetHashCode() % _storeConfig.EventHubPartitionCount;
                    if(partitionKey == string.Empty) partitionKey = storeEvent.PartitionId.ToString();
                    var content = JsonConvert.SerializeObject(storeEvent, Formatting.None);
                    var bytes = Encoding.UTF8.GetBytes(content);
                    eventDatas.Add(new EventData(bytes));
                }
             
                await _eventHubClient.SendAsync(eventDatas, partitionKey);
            }
            catch (Exception e)
            {
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
