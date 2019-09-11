using System;
using System.Collections.Generic;
using System.Text;

namespace Hexastore.Graph
{
    public class Patch
    {
        public IEnumerable<Triple> Assert { get; set; }
        public IEnumerable<Triple> Retract { get; set; }
    }
}
