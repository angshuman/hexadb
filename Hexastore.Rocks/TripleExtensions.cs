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
        private static int BYTES_PER_INT = 4;

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
            if(keySpan.SequenceEqual(new ReadOnlySpan<byte>(KeyConfig.ByteS)))
            {
                return SKVPToTriple(iterator.Key(), iterator.Value());
            }
            else if (keySpan.SequenceEqual(new ReadOnlySpan<byte>(KeyConfig.ByteP)))
            {
                return PKVPToTriple(iterator.Key(), iterator.Value());
            }
            else
            {
                return OKVPToTriple(iterator.Key(), iterator.Value());
            }
        }

        private static Triple OKVPToTriple(byte[] key, byte[] value)
        {
            // name .O z isId z O z S z P z Index
            // NAME: 
            ReadOnlySpan<byte> isId, index, tokenType;
            int oStart = 0, oLength = 0, sStart = 0, sLength = 0, pStart = 0, pLength = 0; 
            var dotOSpan = new ReadOnlySpan<byte>(KeyConfig.ByteO);
            var currentIndex = 0;
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (dotOSpan.SequenceEqual(new ReadOnlySpan<byte>(key, i, 2)))
                {
                    currentIndex = i;
                    break;
                }
            }

            // IsId:
            // currentIdex is at '.' in '.O'. Next should be 'z', can skip both 'Oz' to get to isId 
            currentIndex += 3;
            isId = new ReadOnlySpan<byte>(key, currentIndex, 1);
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
            // TokenType:
            tokenType = new ReadOnlySpan<byte>(value);

            return new Triple(Encoding.UTF8.GetString(key, sStart, sLength), Encoding.UTF8.GetString(key, pStart, pLength), new TripleObject(Encoding.UTF8.GetString(key, oStart, oLength), MemoryMarshal.Read<bool>(isId), (JTokenType)MemoryMarshal.Read<ushort>(tokenType), MemoryMarshal.Read<int>(index)));
        }

        private static Triple PKVPToTriple(byte[] key, byte[] value)
        {
            // name .P z p z isId z O z Index z S 
            // NAME: 
            ReadOnlySpan<byte> isId, index, tokenType;
            int oStart = 0, oLength = 0, sStart = 0, sLength = 0, pStart = 0, pLength = 0;

            var dotPSpan = new ReadOnlySpan<byte>(KeyConfig.ByteP);
            var currentIndex = 0;
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (dotPSpan.SequenceEqual(new ReadOnlySpan<byte>(key, i, 2)))
                {
                    currentIndex = i;
                    break;
                }
            }

            // P:
            // currentIndex is at '.' in '.P'. Next should be 'z', can skip both 'Pz' to get to p value
            currentIndex += 3;

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
            isId = new ReadOnlySpan<byte>(key, currentIndex, 1);
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

            // TokenType:
            tokenType = new ReadOnlySpan<byte>(value);

            return new Triple(Encoding.UTF8.GetString(key, sStart, sLength), Encoding.UTF8.GetString(key, pStart, pLength), new TripleObject(Encoding.UTF8.GetString(key, oStart, oLength), MemoryMarshal.Read<bool>(isId), (JTokenType)MemoryMarshal.Read<ushort>(tokenType), MemoryMarshal.Read<int>(index)));
        }

        private static Triple SKVPToTriple(byte[] key, byte[] value)
        {
            // name .S z s z p z Index
            // NAME: 
            ReadOnlySpan<byte> index;
            int sStart = 0, sLength = 0, pStart = 0, pLength = 0;

            var dotSSpan = new ReadOnlySpan<byte>(KeyConfig.ByteS);
            var currentIndex = 0;
            for (var i = currentIndex; i < key.Length; i++)
            {
                if (dotSSpan.SequenceEqual(new ReadOnlySpan<byte>(key, i, 2)))
                {
                    currentIndex = i;
                    break;
                }
            }

            // S:
            // currentIndex is at '.' in '.S'. Next should be 'z', can skip both 'Sz' to get to s value
            currentIndex += 3;

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
