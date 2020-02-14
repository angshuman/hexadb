using Hexastore.Graph;
using Newtonsoft.Json.Linq;
using RocksDbSharp;
using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Hexastore.Rocks
{
    public static class TripleExtensions
    {
        private const int BYTES_PER_INT = 4;

        public static byte[] GetOIsIdTypeBytes(this Triple triple)
        {
            var o = triple.Object;
            var oTyped = o.ToTypedJSON();
            var oTypeBytes = BitConverter.GetBytes((ushort)oTyped.Type);
            var oBytes = GetBytes(o.ToValue());
            var idBytes = o.IsID ? new byte[] { 1 } : new byte[] { 0 };

            var totalLength = (BYTES_PER_INT * 3) + oBytes.Length + idBytes.Length + oTypeBytes.Length;
            var destination = new byte[totalLength];
            var oIdType = new[] { oBytes, idBytes, oTypeBytes };

            var index = 0;
            foreach (var item in oIdType)
            {
                var lenBytes = BitConverter.GetBytes(item.Length);
                Buffer.BlockCopy(lenBytes, 0, destination, index, BYTES_PER_INT);
                index += BYTES_PER_INT;
                Buffer.BlockCopy(item, 0, destination, index, item.Length);
                index += item.Length;
            }

            return destination;
        }

        public static Triple ToTriple(this byte[] spoKey, byte[] spoValue)
        {
            return SKVPToTriple(spoKey, spoValue);
        }

        public static Triple IteratorToTriple(this Iterator iterator)
        {
            var key = iterator.Key();
            var splits = KeyConfig.Split(key);
            var keySpan = new ReadOnlySpan<byte>(splits[0], splits[0].Length - 2, 2);
            if(keySpan.SequenceEqual(KeyConfig.ByteS))
            {
                return SKVPToTriple(iterator.Key(), iterator.Value());
            }
            else if (keySpan.SequenceEqual(KeyConfig.ByteP))
            {
                return PKVPToTriple(iterator.Key(), iterator.Value());
            }
            else
            {
                return OKVPToTriple(iterator.Key(), iterator.Value());
            }
        }

        private static byte[] StringToBool(ReadOnlySpan<byte> input)
        {
            if (input.SequenceEqual(KeyConfig.ByteTrue)){
                return new byte[] { 1 };
            }
            return new byte[] { 0 };
        }

        private static Triple OKVPToTriple(byte[] key, byte[] value)
        {
            // name .O z isId z O z S z P z Index
            // NAME: 
            ReadOnlySpan<byte> isId, index;
            int oStart = 0, oLength = 0, sStart = 0, sLength = 0, pStart = 0, pLength = 0; 
            var currentIndex = 0;
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    // currentIdex is at 'z' in '.Oz'. Next should be isId 
                    currentIndex = i + 1;
                    break;
                }
            }

            // IsId:
            isId = StringToBool(new ReadOnlySpan<byte>(key, currentIndex, 1));
            // currentIdex is at 'IsId'. Next should be 'z', can skip both 'IsId z' to get to o value 
            currentIndex += 2;

            // O:
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    oStart = currentIndex;
                    oLength = i - currentIndex;
                    currentIndex = i;
                    break;
                }
            }
            currentIndex++;

            // S:
            for(var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    sStart = currentIndex;
                    sLength = i - currentIndex;
                    currentIndex = i;
                    break;
                }
            }
            currentIndex++;

            // P:
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    pStart = currentIndex;
                    pLength = i - currentIndex;
                    currentIndex = i;
                    break;
                }
            }

            // Index:
            // currentIdex is at 'z'. Next should be Index 
            currentIndex++;
            index = new ReadOnlySpan<byte>(key, currentIndex, key.Length - currentIndex);
            // TokenType: value

            return new Triple(Encoding.UTF8.GetString(key, sStart, sLength), Encoding.UTF8.GetString(key, pStart, pLength), new TripleObject(Encoding.UTF8.GetString(key, oStart, oLength), MemoryMarshal.Read<bool>(isId), (JTokenType)MemoryMarshal.Read<ushort>(value), MemoryMarshal.Read<int>(index)));
        }

        private static Triple PKVPToTriple(byte[] key, byte[] value)
        {
            // name .P z p z isId z O z Index z S 
            // NAME: 
            ReadOnlySpan<byte> isId, index;
            int oStart = 0, oLength = 0, sStart = 0, sLength = 0, pStart = 0, pLength = 0;

            var currentIndex = 0;
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    // currentIndex is at 'z' in '.Pz'. Next should be p value
                    currentIndex = i + 1;
                    break;
                }
            }

            // P:
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    pStart = currentIndex;
                    pLength = i - currentIndex;
                    currentIndex = i;
                    break;
                }
            }


            // IsId:
            // currentIndex is at 'z'. Next should be be isId 
            currentIndex++;
            isId = StringToBool(new ReadOnlySpan<byte>(key, currentIndex, 1));
            // currentIndex is at 'IsId'. Next should be 'z', can skip both 'IsId z' to get to o value 
            currentIndex += 2;

            // O:
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    oStart = currentIndex;
                    oLength = i - currentIndex;
                    currentIndex = i;
                    break;
                }
            }

            // currentIdex is at 'z'. Next should be Index 
            currentIndex++;
            index = new ReadOnlySpan<byte>(key, currentIndex, BYTES_PER_INT);
            currentIndex += BYTES_PER_INT + 1; // skipping ahead of index z

            // S:
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    sStart = currentIndex;
                    sLength = i - currentIndex;
                    break;
                }
                if (i == key.Length - 1)
                {
                    sStart = currentIndex;
                    sLength = key.Length - currentIndex;
                    break;
                }
            }

            // TokenType: Value

            return new Triple(Encoding.UTF8.GetString(key, sStart, sLength), Encoding.UTF8.GetString(key, pStart, pLength), new TripleObject(Encoding.UTF8.GetString(key, oStart, oLength), MemoryMarshal.Read<bool>(isId), (JTokenType)MemoryMarshal.Read<ushort>(value), MemoryMarshal.Read<int>(index)));
        }

        private static Triple SKVPToTriple(byte[] key, byte[] value)
        {
            // name .S z s z p z Index
            // NAME: 
            ReadOnlySpan<byte> index;
            int sStart = 0, sLength = 0, pStart = 0, pLength = 0;

            var currentIndex = 0;
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    // currentIndex is at 'z' in '.Sz'. Next should be s value
                    currentIndex = i + 1;
                    break;
                }
            }

            // S:
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    sStart = currentIndex;
                    sLength = i - currentIndex;
                    currentIndex = i;
                    break;
                }
            }
            currentIndex++;

            // P:
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (key[i] == 0)
                {
                    pStart = currentIndex;
                    pLength = i - currentIndex;
                    currentIndex = i;
                    break;
                }
            }

            // currentIndex is at 'z'. Next should be Index 
            currentIndex++;
            // Index;
            index = new ReadOnlySpan<byte>(key, currentIndex, key.Length - currentIndex);

            //value = olength o idLength id typeLength type

            currentIndex = 0;
            var (o, olen) = ReadString(value, currentIndex);
            currentIndex += BYTES_PER_INT + olen;
            var (isId, idLen) = ReadBool(value, currentIndex);
            currentIndex += BYTES_PER_INT + idLen;
            var (tokenType, _) = ReadUInt16(value, currentIndex);

            return new Triple(Encoding.UTF8.GetString(key, sStart, sLength), Encoding.UTF8.GetString(key, pStart, pLength), new TripleObject(o, isId, (JTokenType)tokenType, MemoryMarshal.Read<int>(index)));
        }

        private static byte[] ReadNext(byte[] source, int index)
        {
            var lenSpan = new ReadOnlySpan<byte>(source, index, BYTES_PER_INT);
            var len = MemoryMarshal.Read<int>(lenSpan);
            var destination = new ReadOnlySpan<byte>(source, index + BYTES_PER_INT, len);
            return destination.ToArray();
        }

        private static (string, int) ReadString(byte[] source, int index)
        {
            var lenSpan = new ReadOnlySpan<byte>(source, index, BYTES_PER_INT);
            var len = MemoryMarshal.Read<int>(lenSpan);
            return (Encoding.UTF8.GetString(source, index + BYTES_PER_INT, len), len);
        }

        private static (bool, int) ReadBool(byte[] source, int index)
        {
            var valueSpan = new ReadOnlySpan<byte>(source, index + BYTES_PER_INT, 1);
            return (MemoryMarshal.Read<bool>(valueSpan), 1);
        }

        private static (int, int) ReadInt(byte[] source, int index)
        {
            var lenSpan = new ReadOnlySpan<byte>(source, index, BYTES_PER_INT);
            var len = MemoryMarshal.Read<int>(lenSpan);
            var valueSpan = new ReadOnlySpan<byte>(source, index + BYTES_PER_INT, len);
            return (MemoryMarshal.Read<int>(valueSpan), len);
        }

        private static (ushort, int) ReadUInt16(byte[] source, int index)
        {
            var lenSpan = new ReadOnlySpan<byte>(source, index, BYTES_PER_INT);
            var len = MemoryMarshal.Read<int>(lenSpan);
            var valueSpan = new ReadOnlySpan<byte>(source, index + BYTES_PER_INT, len);
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
