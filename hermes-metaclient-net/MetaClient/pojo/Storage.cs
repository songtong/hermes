using System.Collections.Generic;
using System.Runtime.Serialization;

namespace com.ctrip.hermes.meta.client.pojo
{
    public class Storage
    {
        public string type { get; set; }

        public List<Property> properties { get; set; }

        public List<Datasource> datasources { get; set; }

        public Dictionary<int, Partition> partitions { get; set; }
    }
}
