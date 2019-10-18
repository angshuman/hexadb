using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Hexastore.Web.EventHubs
{
    public class EventType
    {
        public const string POST = "POST";
        public const string PATCH_JSON = "PATCH_JSON";
        public const string PATCH_TRIPLE = "PATCH_TRIPLE";
        public const string DELETE = "DELETE";
    }
}
