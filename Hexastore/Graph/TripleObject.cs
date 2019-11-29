using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;

namespace Hexastore.Graph
{
    public class TripleObject
    {
        public string Value { get; }
        public JTokenType TokenType { get; }
        public bool IsID { get; }
        public int Index { get; }

        public TripleObject(string id, int? arrayIndex)
            : this(id, true, JTokenType.String, arrayIndex)
        {
        }

        public TripleObject(JValue data, bool isId, int? arrayIndex)
        {
            IsID = isId;
            TokenType = data.Type;
            Value = data.Value<string>();
            Index = arrayIndex == null ? -1 : (int)arrayIndex;
        }

        public TripleObject(string value, bool isID, JTokenType tokenType, int? arrayIndex)
        {
            Value = value;
            IsID = isID;
            TokenType = tokenType;
            Index = arrayIndex == null ? -1 : (int)arrayIndex;
        }

        [JsonIgnore]
        public string Id
        {
            get
            {
                if (IsID) {
                    return Value;
                }
                throw new InvalidOperationException("object is not an Id");
            }
        }
        [JsonIgnore]
        public bool IsNull
        {
            get
            {
                return Value == null;
            }
        }

        public static implicit operator TripleObject(JToken jToken)
        {
            return new TripleObject((JValue)jToken, false, null);
        }

        public static implicit operator TripleObject(string id)
        {
            return new TripleObject(id, true, JTokenType.String, null);
        }

        public static TripleObject FromId(string s)
        {
            return new TripleObject(s, null);
        }

        public static TripleObject FromData(string s)
        {
            return new TripleObject(s, false, JTokenType.String, null);
        }

        public static TripleObject FromData(int n)
        {
            return new TripleObject($"{n}", false, JTokenType.Integer, null);
        }

        public static TripleObject FromData(long n)
        {
            return new TripleObject($"{n}", false, JTokenType.Float, null);
        }

        public static TripleObject FromData(double n)
        {
            return new TripleObject($"{n}", false, JTokenType.Float, null);
        }

        public static TripleObject FromData(bool f)
        {
            return new TripleObject(f ? "true" : "false", false, JTokenType.Boolean, null);
        }

        public static TripleObject FromRaw(string json)
        {
            return new TripleObject(json, false, JTokenType.String, null);
        }

        public static string Stringify(JValue jValue)
        {
            using (var textWriter = new StringWriter()) {
                using (var jsonWriter = new JsonTextWriter(textWriter)) {
                    jValue.WriteTo(jsonWriter);
                    jsonWriter.Flush();
                    return textWriter.ToString();
                }
            }
        }

        public override string ToString()
        {
            if (IsID) {
                return $"<{Value}>";
            } else {
                return Value;
            }
        }

        public string ToValue()
        {
            return Value;
        }

        public JValue ToTypedJSON()
        {
            switch (TokenType) {
                case JTokenType.String:
                case JTokenType.Date:
                case JTokenType.TimeSpan:
                case JTokenType.Guid:
                case JTokenType.Uri:
                    return new JValue(Value);
                case JTokenType.Integer:
                    return new JValue(int.Parse(Value));
                case JTokenType.Boolean:
                    return new JValue(bool.Parse(Value));
                case JTokenType.Float:
                    return new JValue(float.Parse(Value));
                default:
                    throw new InvalidOperationException($"{TokenType} not support as object");
            }
        }

        public string DataAsString()
        {
            if (IsID) {
                throw new InvalidOperationException("object is an id but should be data");
            }
            return JValue.Parse(Value).ToString();
        }

        public override bool Equals(object obj)
        {
            var t = obj as TripleObject;
            if (t == null) {
                return false;
            }

            if (Value == null && t.Value == null) {
                return true;
            }

            //if (t.Index == -1 || Index == -1) {
            //    // unordered comparison
            //    return t.IsID == IsID && t.Value == Value && t.TokenType == TokenType;
            //}

            return t.IsID == IsID && t.Value == Value && t.Index == Index && t.TokenType == TokenType;
        }

        public override int GetHashCode()
        {
            return (IsID ? "1" : "0" + Value).GetHashCode();
        }
    }
}
