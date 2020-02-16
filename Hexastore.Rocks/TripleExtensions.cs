using Hexastore.Graph;
using Newtonsoft.Json.Linq;
using System;
using System.Text;

namespace Hexastore.Rocks
{
    public static class TripleExtensions
    {
        public static Triple ToTriple(this (byte[], byte[]) input)
        {
            var key = input.Item1;
            var value = input.Item2;

            byte z = KeyConfig.ByteZero[0];
            var firstZPos = Array.IndexOf(key, z);
            string s;
            string p;
            string o;
            int index;
            bool isId;
            JTokenType type;

            int sEnd;
            int pEnd;
            int oEnd;
            int isIdEnd;

            switch (key[firstZPos - 1]) {
                case KeyConfig.SMark:
                    sEnd = Array.IndexOf(key, z, firstZPos + 1);
                    s = Encoding.UTF8.GetString(key, firstZPos + 1, sEnd - firstZPos - 1);
                    pEnd = Array.IndexOf(key, z, sEnd + 1);
                    p = Encoding.UTF8.GetString(key, sEnd + 1, pEnd - sEnd - 1);
                    index = BitConverter.ToInt32(key, pEnd + 1);
                    var to = value.ToTripleObject();
                    o = to.Value;
                    isId = to.IsID;
                    type = to.TokenType;
                    break;
                case KeyConfig.PMark:
                    pEnd = Array.IndexOf(key, z, firstZPos + 1);
                    p = Encoding.UTF8.GetString(key, firstZPos + 1, pEnd - firstZPos - 1);
                    isId = key[pEnd + 1] == KeyConfig.ByteTrue[0] ? true : false;
                    isIdEnd = pEnd + 2;
                    oEnd = Array.IndexOf(key, z, isIdEnd + 1);
                    o = Encoding.UTF8.GetString(key, isIdEnd + 1, oEnd - isIdEnd - 1);
                    sEnd = Array.IndexOf(key, z, oEnd + 1);
                    s = Encoding.UTF8.GetString(key, oEnd + 1, sEnd - oEnd - 1);
                    index = BitConverter.ToInt32(key, sEnd + 1);
                    type = (JTokenType)BitConverter.ToInt32(value, 0);
                    break;
                case KeyConfig.OMark:
                    isIdEnd = firstZPos + 2;
                    isId = key[firstZPos + 1] == KeyConfig.ByteTrue[0] ? true : false;
                    oEnd = Array.IndexOf(key, z, isIdEnd + 1);
                    o = Encoding.UTF8.GetString(key, isIdEnd + 1, oEnd - isIdEnd - 1);
                    sEnd = Array.IndexOf(key, z, oEnd + 1);
                    s = Encoding.UTF8.GetString(key, oEnd + 1, sEnd - oEnd - 1);
                    pEnd = Array.IndexOf(key, z, sEnd + 1);
                    p = Encoding.UTF8.GetString(key, sEnd + 1, pEnd - sEnd - 1);
                    index = BitConverter.ToInt32(key, pEnd + 1);
                    type = (JTokenType)BitConverter.ToInt32(value, 0);
                    break;
                default:
                    throw new InvalidOperationException("unmatched key. panic.");
            }

            return new Triple(s, p, new TripleObject(o, isId, type, index));
        }
    }
}
