using System;
using System.Collections.Generic;
using System.Text;
using Hexastore.Web.Errors;

namespace Hexastore.Errors
{
    public class StoreError
    {
        public string InvalidTypeError => "400.001";
        public string InvalidItemError => "400.002";
        public string MustHaveIdError => "400.003";
        public string AtLestOneFilterError => "400.004";
        public string PathEmptyError => "400.005";
        public string UnknownCOmparatorError => "400.006";
        public string UnableToParseQuery => "400.007";
        public string AlreadyContainsIdError => "409.001";
        public string Unhandled => "500.001";
        public StoreException InvalidType => new StoreException("Valid input types should be array or object", InvalidTypeError);
        public StoreException InvalidItem => new StoreException("Input array should be made of objects", InvalidItemError);
        public StoreException MustHaveId => new StoreException("Must have a top level string id", MustHaveIdError);
        public StoreException AtLeastOneFilter => new StoreException("need at least one filter", AtLestOneFilterError);
        public StoreException PathEmpty => new StoreException("path cannot be empty", PathEmptyError);
        public StoreException UnknownComparator => new StoreException("Unknown Comparator Type", UnknownCOmparatorError);
    }
}
