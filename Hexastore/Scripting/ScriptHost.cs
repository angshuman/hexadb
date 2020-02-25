using System;
using System.Collections.Generic;
using System.Text;
using Hexastore.Processor;
using NLua;

namespace Hexastore.Scripting
{
    public class ScriptHost : IDisposable
    {
        private readonly Lua _lua;

        public ScriptHost(IStoreProcesor storeProcessor)
        {
            _lua = new Lua();
            _lua["store"] = storeProcessor;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
