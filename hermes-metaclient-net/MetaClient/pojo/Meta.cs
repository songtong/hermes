using System.Collections.Generic;

namespace com.ctrip.hermes.meta.client.pojo
{
    public class Meta
    {
        public bool devMode { get; set; }

        public int version { get; set; }

        public Dictionary<string, Topic> topics { get; set; }

        public Dictionary<long, App> apps { get; set; }

        public Dictionary<string, Codec> codecs { get; set; }

        public Dictionary<string, Endpoint> endpoints { get; set; }

        public Dictionary<string, Storage> storages { get; set; }
    }
}
