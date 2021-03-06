﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.Controllers
{
    public class UpdateRequest
    {
        [JsonProperty("partitionKey")]
        public string PartitionKey { get; set; }

        [JsonProperty("data")]
        public JObject Data { get; set; }
    }
}
