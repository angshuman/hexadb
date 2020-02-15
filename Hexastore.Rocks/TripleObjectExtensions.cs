using System;
using System.Runtime.InteropServices;
using System.Text;
using Hexastore.Graph;
using Newtonsoft.Json.Linq;

namespace Hexastore.Rocks
{
    public static class TripleObjectExtensions
    {
        public static byte[] ToBytes(this TripleObject o)
        {
            var oBytes = KeyConfig.GetBytes(o.ToValue());
            var isIdBytes = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            var typeBytes = KeyConfig.GetBytes((int)o.TokenType);

            // index is not converted
            return KeyConfig.ConcatBytes(isIdBytes, typeBytes, oBytes);
        }

        public static TripleObject ToTripleObject(this byte[] input)
        {
            var isID = input[0] == KeyConfig.ByteTrue[0] ? true : false;
            var typeSpan = new ReadOnlySpan<byte>(input, 1, 4);
            var type = MemoryMarshal.Read<int>(typeSpan);
            var valueLength = input.Length - 5;
            var value = Encoding.UTF8.GetString(input, 5, valueLength);
            return new TripleObject(value, isID, (JTokenType)type, null);
        }
    }
}
