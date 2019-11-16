using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Hexastore.Web
{
    public class StoreConfig
    {
        public string EventHubConnectionString { get; }

        public int EventHubPartitionCount { get; }

        public bool ReplicationIsActive { get; }

        public string EventHubName { get; }

        public StoreConfig()
        {
            EventHubConnectionString = Environment.GetEnvironmentVariable("HEXASTORE_EVENTHUB_KEY");
            int.TryParse(Environment.GetEnvironmentVariable("HEXASTORE_EVENTHUB_PARTITION_COUNT"), out var paritionCount);
            EventHubPartitionCount = paritionCount;
            EventHubName = Environment.GetEnvironmentVariable("HEXASTORE_EVENTHUB_NAME");
            if (!string.IsNullOrEmpty(EventHubConnectionString)
                && EventHubPartitionCount != 0
                && !string.IsNullOrEmpty(EventHubName)) {
                ReplicationIsActive = true;
            }
        }
    }
}
