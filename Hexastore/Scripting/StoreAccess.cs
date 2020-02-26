using System;
using System.Collections.Generic;
using System.Text;
using Hexastore.Processor;
using Newtonsoft.Json.Linq;

namespace Hexastore.Scripting
{
    public class StoreAccess
    {
        private readonly string _storeId;
        private readonly IStoreProcesor _storeProcessor;

        public StoreAccess(string storeId, IStoreProcesor storeProcessor)
        {
            _storeId = storeId;
            _storeProcessor = storeProcessor;
        }

        public JObject Get(string subject, string[] expand, int level)
        {
            return _storeProcessor.GetSubject(_storeId, subject, expand, level);
        }
    }
}








