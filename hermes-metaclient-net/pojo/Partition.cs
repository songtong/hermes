
namespace com.ctrip.hermes.meta.client.pojo
{
    public class Partition
    {
        public int id { get; set; }

        public string readDatasource { get; set; }

        public string writeDatasource { get; set; }

        public string endpoint { get; set; }
    }
}
