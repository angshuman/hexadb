using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Hexastore.Test
{
    public static class DataGenerator
    {
        public static JObject GenerateTelemetry(string deviceId, string property, double value)
        {
            return GenerateTelemetry(deviceId, DateTime.UtcNow, new Dictionary<string, double>() { { property, value } }, new Dictionary<string, string>());
        }

        public static JObject GenerateTelemetry(string deviceId, DateTimeOffset time, Dictionary<string, double> numberValues)
        {
            return GenerateTelemetry(deviceId, time, numberValues, new Dictionary<string, string>());
        }

        public static JObject GenerateTelemetry(string deviceId, DateTimeOffset time, Dictionary<string, double> numberValues, Dictionary<string, string> stringValues)
        {
            var json = GenerateEmptyMessage(deviceId, time);
            var body = (JObject)json["data"]["Body"];

            body.Add(numberValues.Select(e => new JProperty(e.Key, e.Value)));
            body.Add(stringValues.Select(e => new JProperty(e.Key, e.Value)));

            return json;
        }

        public static JObject GenerateEmptyMessage(string deviceId, DateTimeOffset time)
        {
            return new JObject(
                new JProperty("id", deviceId),
                new JObject("data", 
                new JProperty("EnqueuedTimeUtc", time.ToString("o")),
                new JProperty("Properties", new JObject()),
                new JProperty("SystemProperties", new JObject(
                    new JProperty("connectionDeviceId", deviceId))),
                new JProperty("Body", new JObject())));
        }
    }
}
