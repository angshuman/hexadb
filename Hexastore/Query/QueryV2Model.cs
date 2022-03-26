namespace Hexastore.Query
{
    using Newtonsoft.Json;
    public class QueryV2Request
    {
        [JsonProperty("query")]
        public string Query { get; set; }

        [JsonProperty("continuation")]
        public Continuation Continuation { get; set; }
    }
}
