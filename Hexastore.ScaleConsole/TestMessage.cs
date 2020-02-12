using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;

namespace Hexastore.ScaleConsole
{
    public class TestMessage
    {
        public string App { get; set; }

        public string Device { get; set; }

        public List<string> PropertyNames { get; set; }

        public JObject Json { get; set; }
    }
}
