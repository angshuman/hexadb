using System;
using System.Collections.Generic;
using System.Text;

namespace Hexastore.Graph
{
    public interface ISPIndexQueryableGraph
    {
        Triple SPI(string s, string p, int index);
    }
}
