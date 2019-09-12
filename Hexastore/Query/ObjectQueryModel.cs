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
        public Continuation Continuation { get; set; }
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
        public Continuation Continuation { get; set; }
    }

    public class Continuation
    {
        [JsonProperty("s")]
        public string S { get; set; }
        [JsonProperty("p")]
        public string P { get; set; }
        [JsonProperty("o")]
        public JValue O { get; set; }
        [JsonProperty("i")]
        public bool IsId { get; set; }

        public static implicit operator Continuation(Triple t)
        {
            return new Continuation() { S = t.Subject, P = t.Predicate, O = t.Object.ToTypedJSON(), IsId = t.Object.IsID };
        }

        public override bool Equals(object obj)
        {
            var t = obj as Continuation;
            if (t == null) {
                return false;
            }
            if (object.ReferenceEquals(this, t)) {
                return true;
            }
            return t.S.Equals(S) && t.P.Equals(P) && t.O.CompareTo(O) == 0 && t.IsId == IsId;
        }
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
