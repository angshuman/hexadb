// Copyright (c) Microsoft Corporation. All rights reserved.

using System;
using System.Globalization;
using System.Text;

namespace Hexastore.Web.Queue
{
    /// <summary>
    /// Provides non-cryptographic hashing functions.
    /// </summary>
    public static class Hasher
    {
        private const uint FnvPrime32 = 0x01000193;
        private const uint FnvOffset32 = 0x811C9DC5;

        private const ulong FnvPrime64 = 0x100000001b3;
        private const ulong FnvOffset64 = 0xcbf29ce484222325;

        /// <summary>
        /// Gets a FNV-1a 32-bit hash of the provided <paramref name="content"/>. The FNV-1a algorithm
        /// is used in many context including DNS servers, database indexing hashes, non-cryptographic file
        /// fingerprints to name a few. For more information about FNV, please see the IETF document
        /// <c>The FNV Non-Cryptographic Hash Algorithm</c> as well as <c>http://isthe.com/chongo/tech/comp/fnv/</c>.
        /// </summary>
        /// <param name="content">The content to hash.</param>
        /// <returns>The computed hash.</returns>
        public static int GetFnvHash32(string content)
        {
            if (content == null) {
                throw new ArgumentNullException("content");
            }
            byte[] data = Encoding.UTF8.GetBytes(content);

            uint hash = FnvOffset32;
            for (int cnt = 0; cnt < data.Length; cnt++) {
                unchecked {
                    hash ^= data[cnt];
                    hash = hash * FnvPrime32;
                }
            }
            return (int)hash;
        }

        /// <summary>
        /// Gets a string representation of a FNV-1a 32-bit hash of the provided <paramref name="content"/>. The FNV-1a
        /// algorithm is used in many context including DNS servers, database indexing hashes, non-cryptographic file
        /// fingerprints to name a few. For more information about FNV, please see the IETF document
        /// <c>The FNV Non-Cryptographic Hash Algorithm</c> as well as <c>http://isthe.com/chongo/tech/comp/fnv/</c>.
        /// </summary>
        /// <param name="content">The content to hash.</param>
        /// <returns>A string representation of the computed hash.</returns>
        public static string GetFnvHash32AsString(string content)
        {
            return ((uint)GetFnvHash32(content)).ToString("x8", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Gets a FNV-1a 64-bit hash of the provided <paramref name="content"/>. The FNV-1a algorithm
        /// is used in many context including DNS servers, database indexing hashes, non-cryptographic file
        /// fingerprints to name a few. For more information about FNV, please see the IETF document
        /// <c>The FNV Non-Cryptographic Hash Algorithm</c> as well as <c>http://isthe.com/chongo/tech/comp/fnv/</c>.
        /// </summary>
        /// <param name="content">The content to hash.</param>
        /// <returns>The computed hash.</returns>
        public static long GetFnvHash64(string content)
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
            return (long)hash;
        }

        /// <summary>
        /// Gets a string representation of a FNV-1a 64-bit hash of the provided <paramref name="content"/>. The FNV-1a
        /// algorithm is used in many context including DNS servers, database indexing hashes, non-cryptographic file
        /// fingerprints to name a few. For more information about FNV, please see the IETF document
        /// <c>The FNV Non-Cryptographic Hash Algorithm</c> as well as <c>http://isthe.com/chongo/tech/comp/fnv/</c>.
        /// </summary>
        /// <param name="content">The content to hash.</param>
        /// <returns>A string representation of the computed hash.</returns>
        public static string GetFnvHash64AsString(string content)
        {
            return ((ulong)GetFnvHash64(content)).ToString("x16", CultureInfo.InvariantCulture);
        }
    }
}
