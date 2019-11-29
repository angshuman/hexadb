using System;
using System.Collections.Generic;
using System.Text;
using Hexastore.Graph;
using System.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hexastore.Test
{
    public class UnorderedTripleComparer : IComparer
    {
        public int Compare(object first, object second)
        {
            var x = first as Triple;
            var y = second as Triple;

            if (x == null || y == null) {
                throw new InvalidOperationException("cannot compare null");
            }

            if (x.Subject == y.Subject && x.Predicate == y.Predicate && x.Object.IsID == y.Object.IsID && x.Object.TokenType == y.Object.TokenType && x.Object.Value == y.Object.Value) {
                return 0;
            }

            return string.Compare(x.Object.Value, y.Object.Value);
        }
    }

    public static class ListExtension
    {
        public static void Assert(this List<Triple> list, string s, string p, TripleObject o)
        {
            list.Add(new Triple(s, p, o));
        }
    }
}
