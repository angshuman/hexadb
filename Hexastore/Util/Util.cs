using Hexastore.Store;

public static class Util
{
    public static string MakeKey(string id, GraphType type)
    {
        return $"{type}:{id}";
    }
}