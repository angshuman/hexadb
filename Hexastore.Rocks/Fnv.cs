using System;
using System.Globalization;
using System.Text;

namespace Hexastore.Rocks
{
    public static class Fnv
    {
        private const ulong FnvPrime64 = 0x100000001b3;
        private const ulong FnvOffset64 = 0xcbf29ce484222325;

        public static ulong GetFnvHash64(string content)
        {
            if (content == null) {
                throw new ArgumentNullException("content");
            }
            byte[] data = Encoding.UTF8.GetBytes(content);

            ulong hash = FnvOffset64;
            for (int cnt = 0; cnt < data.Length; cnt++) {
                unchecked {
                    hash ^= data[cnt];
                    hash = hash * FnvPrime64;
                }
            }
            return hash;
        }

        public static string GetFnvHash64AsString(string content)
        {
            return GetFnvHash64(content).ToString("x16", CultureInfo.InvariantCulture);
        }
    }
}
