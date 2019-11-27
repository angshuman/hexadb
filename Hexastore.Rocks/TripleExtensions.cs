using Hexastore.Graph;
using Newtonsoft.Json.Linq;
using System;
using System.Text;

namespace Hexastore.Rocks
{
    public static class TripleExtensions
    {
        public static byte[] ToBytes(this Triple triple)
        {
            var s = triple.Subject;
            var p = triple.Predicate;
            var o = triple.Object;
            var sBytes = GetBytes(s);
            var pBytes = GetBytes(p);
            var oTyped = o.ToTypedJSON();
            var oTypeBytes = BitConverter.GetBytes((ushort)oTyped.Type);
            var oBytes = GetBytes(o.ToValue());
            var idBytes = o.IsID ? new byte[] { 1 } : new byte[] { 0 };
            var indexBytes = BitConverter.GetBytes(o.Index);

            var totalLength = (4 * 6) + sBytes.Length + pBytes.Length + oBytes.Length + idBytes.Length + oTypeBytes.Length + indexBytes.Length;
            var destination = new byte[totalLength];
            var spio = new[] { sBytes, pBytes, indexBytes, idBytes, oTypeBytes, oBytes };

            var index = 0;
            foreach (var item in spio) {
                var lenBytes = BitConverter.GetBytes(item.Length);
                Buffer.BlockCopy(lenBytes, 0, destination, index, 4);
                index += 4;
                Buffer.BlockCopy(item, 0, destination, index, item.Length);
                index += item.Length;
            }

            return destination;
        }

        public static Triple ToTriple(this byte[] spoBytes)
        {
            var index = 0;
            var sBytes = ReadNext(spoBytes, index);
            index += 4 + sBytes.Length;
            var pBytes = ReadNext(spoBytes, index);
            index += 4 + pBytes.Length;
            var arrayIndexBytes = ReadNext(spoBytes, index);
            index += 4 + arrayIndexBytes.Length;
            var idBytes = ReadNext(spoBytes, index);
            index += 4 + idBytes.Length;
            var oTypeBytes = ReadNext(spoBytes, index);
            index += 4 + oTypeBytes.Length;
            var oBytes = ReadNext(spoBytes, index);

            var s = GetString(sBytes);
            var p = GetString(pBytes);
            var arrayIndex = BitConverter.ToInt32(arrayIndexBytes);
            var ovalue = GetString(oBytes);
            var id = idBytes[0] == 1 ? true : false;
            var type = (JTokenType)BitConverter.ToUInt16(oTypeBytes);
            return new Triple(s, p, new TripleObject(ovalue, id, type, arrayIndex));
        }

        private static byte[] ReadNext(byte[] source, int index)
        {
            var lenBytes = new byte[4];
            Buffer.BlockCopy(source, index, lenBytes, 0, 4);
            var len = BitConverter.ToInt32(lenBytes, 0);

            var destination = new byte[len];
            Buffer.BlockCopy(source, index + 4, destination, 0, len);
            return destination;
        }

        private static byte[] GetBytes(string str)
        {
            // todo: optimize GetBytes for known patterns
            return Encoding.UTF8.GetBytes(str);
        }

        private static string GetString(byte[] bytes)
        {
            return Encoding.UTF8.GetString(bytes);
        }
    }
}
