using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.Controllers
{
    public class UpdateRequest
    {
        [JsonProperty("partitionId")]
        public string PartitionId { get; set; }

        [JsonProperty("data")]
        public JToken Data { get; set; }
    }
}
