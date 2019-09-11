using Hexastore.Graph;

namespace Hexastore.Resoner
{
    public interface IReasoner
    {
        void Spin(IGraph data, IGraph inferred, IGraph meta);
    }
}