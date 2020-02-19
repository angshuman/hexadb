using Hexastore.Web.EventHubs;

namespace Hexastore.Web.Queue
{
    public interface IQueueContainer
    {
        public void Send(StoreEvent storeEvent);
        public int Count();
    }
}
