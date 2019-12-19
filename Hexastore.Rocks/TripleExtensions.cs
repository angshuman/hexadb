using Hexastore.Graph;
using Newtonsoft.Json.Linq;
using System;
using System.Runtime.InteropServices;
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
            var (s, slen) = ReadString(spoBytes, index);
            index += 4 + slen;
            var (p, pLen) = ReadString(spoBytes, index);
            index += 4 + pLen;
            var (arrayIndex, arrayIndexLength) = ReadInt(spoBytes, index);
            index += 4 + arrayIndexLength;
            var (id, idLen) = ReadBool(spoBytes, index);
            index += 4 + idLen;
            var (type, typeLength) = ReadUInt16(spoBytes, index);
            index += 4 + typeLength;
            var (ovalue, _) = ReadString(spoBytes, index);

            return new Triple(s, p, new TripleObject(ovalue, id, (JTokenType)type, arrayIndex));
        }

        private static byte[] ReadNext(byte[] source, int index)
        {
            var lenSpan = new ReadOnlySpan<byte>(source, index, 4);
            var len = MemoryMarshal.Read<int>(lenSpan);
            var destination = new ReadOnlySpan<byte>(source, index + 4, len);
            return destination.ToArray();
        }

        private static (string, int) ReadString(byte[] source, int index)
        {
            var lenSpan = new ReadOnlySpan<byte>(source, index, 4);
            var len = MemoryMarshal.Read<int>(lenSpan);
            return (Encoding.UTF8.GetString(source, index + 4, len), len);
        }

        private static (bool, int) ReadBool(byte[] source, int index)
        {
            var valueSpan = new ReadOnlySpan<byte>(source, index + 4, 1);
            return (MemoryMarshal.Read<bool>(valueSpan), 1);
        }

        private static (int, int) ReadInt(byte[] source, int index)
        {
            var lenSpan = new ReadOnlySpan<byte>(source, index, 4);
            var len = MemoryMarshal.Read<int>(lenSpan);
            var valueSpan = new ReadOnlySpan<byte>(source, index + 4, len);
            return (MemoryMarshal.Read<int>(valueSpan), len);
        }

        private static (ushort, int) ReadUInt16(byte[] source, int index)
        {
            var lenSpan = new ReadOnlySpan<byte>(source, index, 4);
            var len = MemoryMarshal.Read<int>(lenSpan);
            var valueSpan = new ReadOnlySpan<byte>(source, index + 4, len);
            return (MemoryMarshal.Read<ushort>(valueSpan), len);
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
