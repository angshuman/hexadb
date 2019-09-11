using System;
using System.Collections.Generic;
using System.Text;
using Hexastore.Web.Errors;

namespace Hexastore.Errors
{
    public class StoreError
    {
        public StoreException InvalidType => new StoreException("Valid input types should be array or object", "400.001");
        public StoreException InvalidItem => new StoreException("Input array should be made of objects", "400.002");
        public StoreException MustHaveId => new StoreException("Must have a top level string id", "400.003");
        public StoreException AtLeastOneFilter => new StoreException("need at least one filter", "400.005");
        public StoreException PathEmpty => new StoreException("path cannot be empty", "400.006");
        public StoreException UnknownComparator => new StoreException("Unknown Comparator Type", "400.006");
    }
}
