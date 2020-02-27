using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Graph;
using Hexastore.Processor;
using NLua;

namespace Hexastore.Scripting
{
    public class ScriptHost : IDisposable
    {
        private readonly Lua _lua;
        private readonly IStoreProcesor _storeProcessor;

        public void Load(string script, string storeId)
        {
            _lua["store"] = new StoreAccess(storeId, _storeProcessor);
            _lua.DoString(script);
        }

        public bool? Filter(string subjectId, string functionName)
        {
            var luaFunction = _lua[functionName] as LuaFunction;
            var rsp = luaFunction.Call(subjectId).First();
            return rsp as bool?;
        }

        public void Dispose()
        {
            _lua.Dispose();
        }
    }
}
