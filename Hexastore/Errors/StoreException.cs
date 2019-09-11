using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Hexastore.Web.Errors
{
    public class StoreException : Exception
    {
        public StoreException(string message, string errorCode) : base(message)
        {
            ErrorCode = errorCode;
        }
        public string ErrorCode { get; set; }
    }
}
