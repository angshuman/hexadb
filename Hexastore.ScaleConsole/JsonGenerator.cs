using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Hexastore.TestCommon
{
    public static class JsonGenerator
    {
        public static JObject GenerateTelemetry(string id, Dictionary<string, double> numberValues)
        {
            return new JObject(new JProperty("id", id),
                new JProperty("data", new JObject(
                    numberValues.Select(e => new JProperty(e.Key, e.Value)))));
        }

        public static JObject GenerateTelemetry(string id, Dictionary<string, double> numberValues, Dictionary<string, string> stringValues)
        {
            return new JObject(new JProperty("id", id),
                new JProperty("data", new JObject(
                    numberValues.Select(e => new JProperty(e.Key, e.Value)),
                    stringValues.Select(e => new JProperty(e.Key, e.Value)))));
        }
    }
}
