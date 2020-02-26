using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Processor;
using NLua;

namespace Hexastore.Scripting
{
    public class ScriptHost : IScriptHost, IDisposable
    {
        private readonly Lua _lua;
        private readonly IStoreProcesor _storeProcessor;

        public ScriptHost(IStoreProcesor storeProcessor)
        {
            _storeProcessor = storeProcessor;
        }

        public bool? Filter(string storeId, string subjectId, string script, string functionName)
        {
            using(var lua = new Lua()) {
                lua["store"] = new StoreAccess(storeId, _storeProcessor);
                lua.DoString(script);
                var luaFunction = lua[functionName] as LuaFunction;
                var rsp = luaFunction.Call(subjectId).First();
                return rsp as bool?;
            }
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
