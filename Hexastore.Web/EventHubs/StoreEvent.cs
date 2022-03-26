using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.EventHubs
{
    public class StoreEvent
    {
        [JsonProperty("operation")]
        public string Operation { get; set; }

        [JsonProperty("operationId")]
        public string OperationId { get; set; }

        [JsonProperty("data")]
        public JToken Data { get; set; }

        [JsonProperty("strict")]
        public bool Strict { get; set; }

        [JsonProperty("partitionId")]
        public string PartitionId { get; set; }

        [JsonProperty("storeId")]
        public string StoreId { get; set; }

        public static StoreEvent FromString(string str)
        {
            try {
                return JsonConvert.DeserializeObject<StoreEvent>(str);
            } catch (Exception e) {
                throw new InvalidOperationException("Cannot parse store event", e);
            }
        }
    }

    public class BatchedStoreEvent : StoreEvent
    {
        [JsonProperty("data")]
        public JArray Batch { get; set; }
    }
}
