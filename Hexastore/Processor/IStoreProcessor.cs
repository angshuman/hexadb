namespace Hexastore.Processor
{
    using System.Collections.Generic;
    using Hexastore.Graph;
    using Newtonsoft.Json.Linq;

    public interface IStoreProcessor
    {
        void Assert(string storeId, IEnumerable<Triple> triples);
        void Patch(string storeId, IEnumerable<Triple> triples);
        void Assert(string storeId, JToken value, bool strict);
        void PatchJson(string storeId, JToken input);
        void PatchTriple(string storeId, JObject input);
        void AssertMeta(string storeId, JObject value);
        void Delete(string storeId, JToken value);
        void CreateTwin(string storeId, JToken data);
        void CreateRelationship(string storeId, JToken data);

        JObject GetSet(string storeId);
        (IStoreGraph, IStoreGraph, IStoreGraph) GetGraphs(string storeId);
        JObject GetSubject(string storeId, string subject, string[] expand, int level);
        JObject GetType(string storeId, string[] type, string[] expand, int level);
        JObject Query(string storeId, JObject query, string[] expand, int level);
        JObject QueryV2(string storeId, JObject query, string[] expand, int level);
        JObject GetPredicates(string storeId);
    }
}
