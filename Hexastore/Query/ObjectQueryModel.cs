using System;
using System.Collections.Generic;
using System.Text;
using Hexastore.Graph;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Hexastore.Query
{
    public class ObjectQueryModel
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        [JsonProperty("filter")]
        public IDictionary<string, QueryUnit> Filter { get; set; }
        [JsonProperty("continuation")]
        public Triple Continuation { get; set; }
        [JsonProperty("pageSize")]
        public int PageSize { get; set; }
        [JsonProperty("incoming")]
        public LinkQuery[] HasSubject { get; set; }
        [JsonProperty("outgoing")]
        public LinkQuery[] HasObject { get; set; }
    }

    public class ObjectQueryResponse
    {
        public IEnumerable<Triple> Values { get; set; }
        public Triple Continuation { get; set; }
    }

    public class QueryUnit
    {
        [JsonProperty("op")]
        public string Operator { get; set; }
        [JsonProperty("value")]
        public object Value { get; set; }
    }

    public class LinkQuery
    {
        [JsonProperty("level")]
        public int Level { get; set; }
        [JsonProperty("path")]
        public string Path { get; set; }
        [JsonProperty("target")]
        public ObjectQueryModel Target { get; set; }
    }
}
