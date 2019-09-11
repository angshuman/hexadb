using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hexastore.Graph;
using Hexastore.Store;
using Newtonsoft.Json.Linq;

namespace Hexastore.Web.EventHubs
{
    public class Checkpoint
    {
        private readonly IGraphProvider _graphProvider;

        public Checkpoint(IGraphProvider graphProvider)
        {
            _graphProvider = graphProvider;
        }

        public void Write(string key, string offset)
        {
            _graphProvider.WriteKey(key, offset);
        }

        public string Get(string key)
        {
            var value = _graphProvider.ReadKey(key);
            if (value == null) {
                return "-1";
            }
            return value;
        }
    }
}
