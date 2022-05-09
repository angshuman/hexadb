using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.GRPC;

namespace Hexastore.Client
{
    public static class TripleExtensions
    {
        public static string Serialize(this TripleMessage tm)
        {
            var rsp = new StringBuilder();
            rsp.Append($"<{tm.Subject}> {tm.Predicate} ");

            if (!string.IsNullOrEmpty(tm.Object)) {
                rsp.Append($"<{tm.Object}>");
                return rsp.ToString();
            }

            switch (tm.Type) {
                case TripleMessage.Types.ValueType.Bool:
                    rsp.Append(tm.BoolValue);
                    break;
                case TripleMessage.Types.ValueType.String:
                    rsp.Append(tm.StringValue);
                    break;
                case TripleMessage.Types.ValueType.Double:
                    rsp.Append(tm.DoubleValue);
                    break;
                case TripleMessage.Types.ValueType.Int:
                    rsp.Append(tm.IntValue);
                    break;
                default:
                    throw new ArgumentException("Unknonw type");
            }

            return rsp.ToString();
        }
    }

    public static class EnumerableExtensions
    {
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> sequence, int batchSize)
        {
            var items = new T[batchSize];
            var count = 0;

            foreach (var i in sequence) {
                items[count++] = i;
                if (count == batchSize) {
                    yield return items;
                    items = new T[batchSize];
                    count = 0;
                }
            }

            if (count > 0) {
                yield return items.Take(count);
            }
        }
    }
}
