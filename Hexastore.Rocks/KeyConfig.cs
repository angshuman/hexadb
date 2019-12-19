using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hexastore.Graph;

namespace Hexastore.Rocks
{
    public class KeySegments
    {
        public byte[] S { get; private set; }
        public byte[] P { get; private set; }
        public byte[] O { get; private set; }
        public byte[] IsId { get; private set; }
        public byte[] Index { get; private set; }

        private byte[] _sKey;
        private byte[] _pKey;
        private byte[] _oKey;

        private byte[] _name;

        public KeySegments(string name, string s, string p, TripleObject o)
        {
            _name = KeyConfig.GetBytes(name);
            S = KeyConfig.GetBytes(s);
            P = KeyConfig.GetBytes(p);
            O = KeyConfig.GetBytes(o.ToValue());
            IsId = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            Index = BitConverter.GetBytes(o.Index);
        }

        public KeySegments(string name, Triple t) : this(name, t.Subject, t.Predicate, t.Object)
        {
        }

        public (byte[], byte[], byte[]) GetKeys()
        {
            if (_sKey == null) {
                var z = KeyConfig.ByteZero;
                _sKey = KeyConfig.ConcatBytes(_name, KeyConfig.ByteS, z, S, z, P, z, Index);
                _pKey = KeyConfig.ConcatBytes(_name, KeyConfig.ByteP, z, P, z, IsId, z, O, z, Index, z, S);
                _oKey = KeyConfig.ConcatBytes(_name, KeyConfig.ByteO, z, IsId, z, O, z, S, z, P, z, Index);
            }

            return (_sKey, _pKey, _oKey);
        }

        public byte[] GetOPrefix()
        {
            var z = KeyConfig.ByteZero;
            return KeyConfig.ConcatBytes(_name, KeyConfig.ByteO, z, IsId, z, O, z, S, z, P);
        }

        public static byte[] GetNameSKey(string name)
        {
            return KeyConfig.ConcatBytes(KeyConfig.GetBytes(name), KeyConfig.ByteS);
        }

        public static byte[] GetNameSKeySubject(string name, string subject)
        {
            var nameBytes = KeyConfig.GetBytes(name);
            var z = KeyConfig.ByteZero;
            var sBytes = KeyConfig.GetBytes(subject);
            return KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteS, z, sBytes);
        }

        public static byte[] GetNameSKeySubjectPredicate(string name, string subject, string predicate)
        {
            var nameBytes = KeyConfig.GetBytes(name);
            var z = KeyConfig.ByteZero;
            var sBytes = KeyConfig.GetBytes(subject);
            var pBytes = KeyConfig.GetBytes(predicate);
            return KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteS, z, sBytes, z, pBytes);
        }

        public static byte[] GetNameSKeySubjectPredicateIndex(string name, string subject, string predicate, int index)
        {
            var nameBytes = KeyConfig.GetBytes(name);
            var z = KeyConfig.ByteZero;
            var sBytes = KeyConfig.GetBytes(subject);
            var pBytes = KeyConfig.GetBytes(predicate);
            var indexBytes = BitConverter.GetBytes(index);
            return KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteS, z, sBytes, z, pBytes, z, indexBytes);
        }

        public static byte[] GetNameOKeyObject(string name, TripleObject o)
        {
            var nameBytes = KeyConfig.GetBytes(name);
            var z = KeyConfig.ByteZero;
            var oBytes = KeyConfig.GetBytes(o.ToValue());
            var isIdBytes = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            return KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteO, z, isIdBytes, z, oBytes);
        }

        public static byte[] GetNameOKeyObjectSubject(string name, TripleObject o, string subject)
        {
            var nameBytes = KeyConfig.GetBytes(name);
            var z = KeyConfig.ByteZero;
            var oBytes = KeyConfig.GetBytes(o.ToValue());
            var isIdBytes = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            var sBytes = KeyConfig.GetBytes(subject);

            return KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteO, z, isIdBytes, z, oBytes, z, sBytes);
        }

        public static byte[] GetNamePKeyPredicate(string name, string predicate)
        {
            var nameBytes = KeyConfig.GetBytes(name);
            var z = KeyConfig.ByteZero;
            var pBytes = KeyConfig.GetBytes(predicate);
            return KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteP, z, pBytes);
        }

        public static byte[] GetNamePPredicate(string name)
        {
            var nameBytes = KeyConfig.GetBytes(name);
            return KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteP);
        }

        public static byte[] GetNamePKeyPredicateObject(string name, string predicate, TripleObject o)
        {
            var nameBytes = KeyConfig.GetBytes(name);
            var z = KeyConfig.ByteZero;
            var pBytes = KeyConfig.GetBytes(predicate);
            var oBytes = KeyConfig.GetBytes(o.ToValue());
            var isIdBytes = o.IsID ? KeyConfig.ByteTrue : KeyConfig.ByteFalse;
            return KeyConfig.ConcatBytes(nameBytes, KeyConfig.ByteP, z, pBytes, z, isIdBytes, z, oBytes);
        }
    }

    public class KeyConfig
    {
        public static byte[] GetBytes(string input)
        {
            return Encoding.UTF8.GetBytes(input);
        }

        public static byte[] ConcatBytes(params byte[][] inputs)
        {
            var totalLength = inputs.Sum(x => x.Length);
            var output = new byte[totalLength];

            var outputIndex = 0;
            foreach (var i in inputs) {
                Buffer.BlockCopy(i, 0, output, outputIndex, i.Length);
                outputIndex += i.Length;
            }
            return output;
        }

        public static int ByteCompare(byte[] first, byte[] second)
        {
            for (int i = 0; i < first.Length; i++) {
                if (i >= second.Length) {
                    return 1;
                }
                var firstByte = first[i];
                var secondByte = second[i];
                if (firstByte < secondByte) {
                    return -1;
                } else if (firstByte > secondByte) {
                    return 1;
                } else {
                    continue;
                }
            }
            if (first.Length == second.Length) {
                return 0;
            }
            return -1;
        }

        public static IList<byte[]> Split(byte[] input)
        {
            if (input.Length == 1 && input[0] != 0) {
                return new byte[][] { input };
            } else if (input.Length == 1 && input[0] == 0) {
                return new byte[][] { };
            }
            var list = new List<byte[]>();
            var lastIndex = -1;
            for (int i = 0; i < input.Length; i++) {
                if (input[i] != 0) {
                    continue;
                }
                var item = new byte[i - lastIndex - 1];
                Buffer.BlockCopy(input, lastIndex + 1, item, 0, item.Length);
                list.Add(item);
                lastIndex = i;
            }
            return list;
        }

        public static readonly byte[] ByteZero = new byte[] { 0 };
        public static readonly byte[] ByteOne = new byte[] { 1 };
        public static readonly byte[] ByteFalse = new byte[] { 48 };
        public static readonly byte[] ByteTrue = new byte[] { 49 };
        public static readonly byte[] ByteS = new byte[] { 46, 83 }; // .S
        public static readonly byte[] ByteP = new byte[] { 46, 80 }; // .P
        public static readonly byte[] ByteO = new byte[] { 46, 79 }; // .O
    }
}
