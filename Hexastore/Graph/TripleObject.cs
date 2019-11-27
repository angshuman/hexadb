using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;

namespace Hexastore.Graph
{
    /// <summary>
    /// Part of the implementation of the flattened graph
    /// (TODO consider not using JSON internally - this shouldn't be too hard now as the usage is now decoupled from the internals)
    /// </summary>
    public class TripleObject
    {
        // TODO: switch to object
        private string _value;
        private readonly JTokenType _tokenType;

        public TripleObject(string id, int? arrayIndex)
            : this(id, true, JTokenType.String, arrayIndex)
        {
        }

        public TripleObject(JValue data, bool isId, int? arrayIndex)
        {
            IsID = isId;
            _tokenType = data.Type;
            _value = data.Value<string>();
            Index = arrayIndex == null ? -1 : (int)arrayIndex;
        }

        public TripleObject(string value, bool isID, JTokenType tokenType, int? arrayIndex)
        {
            _value = value;
            IsID = isID;
            _tokenType = tokenType;
            Index = arrayIndex == null ? -1 : (int)arrayIndex;
        }

        [JsonIgnore]
        public string Id
        {
            get
            {
                if (IsID) {
                    return _value;
                }
                throw new InvalidOperationException("object is not an Id");
            }
        }

        public bool IsID { get; }
        public int Index { get; }

        [JsonIgnore]
        public bool IsNull
        {
            get
            {
                return _value == null;
            }
        }

        /// <summary>
        /// actual property data is stored as JSON
        /// </summary>
        public static implicit operator TripleObject(JToken jToken)
        {
            return new TripleObject((JValue)jToken, false, null);
        }

        /// <summary>
        /// all reference types are held as strings (not Uri because they often differ only in fragment)
        /// </summary>
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

        /// <summary>
        /// create avoiding JValue
        /// </summary>
        public static TripleObject FromData(long n)
        {
            return new TripleObject($"{n}", false, JTokenType.Float, null);
        }

        /// <summary>
        /// create avoiding JValue
        /// </summary>
        public static TripleObject FromData(double n)
        {
            return new TripleObject($"{n}", false, JTokenType.Float, null);
        }

        /// <summary>
        /// create avoiding JValue
        /// </summary>
        public static TripleObject FromData(bool f)
        {
            return new TripleObject(f ? "true" : "false", false, JTokenType.Boolean, null);
        }

        /// <summary>
        /// create avoiding JValue
        /// </summary>
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
                return $"<{_value}>";
            } else {
                return _value;
            }
        }

        public string ToValue()
        {
            return _value;
        }

        public JValue ToTypedJSON()
        {
            switch (_tokenType) {
                case JTokenType.String:
                case JTokenType.Date:
                case JTokenType.TimeSpan:
                case JTokenType.Guid:
                case JTokenType.Uri:
                    return new JValue(_value);
                case JTokenType.Integer:
                    return new JValue(int.Parse(_value));
                case JTokenType.Boolean:
                    return new JValue(bool.Parse(_value));
                case JTokenType.Float:
                    return new JValue(float.Parse(_value));
                default:
                    throw new InvalidOperationException($"{_tokenType} not support as object");
            }
        }

        public string DataAsString()
        {
            if (IsID) {
                throw new InvalidOperationException("object is an id but should be data");
            }
            return JValue.Parse(_value).ToString();
        }

        public override bool Equals(object obj)
        {
            if (_value == null && ((TripleObject)obj)._value == null) {
                return true;
            }
            return ((TripleObject)obj).IsID == IsID && ((TripleObject)obj)._value.Equals(_value);
        }

        public override int GetHashCode()
        {
            return (IsID ? "1" : "0" + _value).GetHashCode();
        }
    }
}
