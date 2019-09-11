using System.Threading;

namespace Hexastore.Parser
{
    public static class IdGenerator
    {
        public static int lastId = 0;

        public static int Generate()
        {
            return Interlocked.Increment(ref lastId);
        }
    }
}
