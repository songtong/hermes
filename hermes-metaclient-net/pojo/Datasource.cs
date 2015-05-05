using System;
using System.Collections.Generic;

namespace com.ctrip.hermes.meta.client.pojo
{
    public class Datasource
    {
        public string id { get; set; }

        public Dictionary<String, Property> properties { get; set; }
    }
}
