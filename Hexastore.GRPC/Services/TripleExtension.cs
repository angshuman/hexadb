
namespace Hexastore.GRPC.Services
{
    using System;
    using Hexastore.Graph;
    using Newtonsoft.Json.Linq;

    public static class TripleExtension
    {
        public static TripleMessage ConvertToTripleMessage(this Triple t)
        {
            var tm = new TripleMessage();
            tm.Subject = t.Subject;
            tm.Predicate = t.Predicate;
            if (t.Object.IsID) {
                tm.Object = t.Object.Id;
                return tm;
            }

            switch (t.Object.TokenType) {
                case JTokenType.String:
                case JTokenType.Date:
                case JTokenType.TimeSpan:
                case JTokenType.Guid:
                case JTokenType.Uri:
                    tm.Type = TripleMessage.Types.ValueType.String;
                    tm.StringValue = (string)t.Object.Value;
                    break;
                case JTokenType.Integer:
                    tm.Type = TripleMessage.Types.ValueType.Int;
                    tm.IntValue = int.Parse(t.Object.Value);
                    break;
                case JTokenType.Boolean:
                    tm.Type = TripleMessage.Types.ValueType.Bool;
                    tm.BoolValue = bool.Parse(t.Object.Value);
                    break;
                case JTokenType.Float:
                    tm.Type = TripleMessage.Types.ValueType.Double;
                    tm.DoubleValue = double.Parse(t.Object.Value);
                    break;
                default:
                    throw new InvalidOperationException($"{t.Object.TokenType} not support as object");
            }

            return tm;
        }

        public static Triple ConvertToTriple(this TripleMessage tm)
        {
            JValue token = GetValue(tm);
            var tripleObject = string.IsNullOrEmpty(tm.Object) ? new TripleObject(token, false, tm.ArrayIndex) : new TripleObject(tm.Object, tm.ArrayIndex);
            return new Triple(tm.Subject, tm.Predicate, tripleObject);
        }

        private static JValue GetValue(TripleMessage item)
        {
            switch (item.Type) {
                case TripleMessage.Types.ValueType.Int:
                    return new JValue(item.IntValue);
                case TripleMessage.Types.ValueType.String:
                    return new JValue(item.StringValue);
                case TripleMessage.Types.ValueType.Double:
                    return new JValue(item.DoubleValue);
                case TripleMessage.Types.ValueType.Bool:
                    return new JValue(item.BoolValue);
                default:
                    throw new ArgumentException("Unknown Value Type");
            }
        }
    }
}
