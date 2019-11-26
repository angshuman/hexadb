using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Hexastore.Rocks
{
    public class KeyConfig
    {
        public static byte[] Hash(string input)
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
    }
}
