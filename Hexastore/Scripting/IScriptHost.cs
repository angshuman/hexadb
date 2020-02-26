using System;
using System.Collections.Generic;
using System.Text;

namespace Hexastore.Scripting
{
    public interface IScriptHost
    {
        IScriptHost Create(string storeId);
        bool Filter(IScriptHost host, string subjectId, string script, string functionName);
    }
}
