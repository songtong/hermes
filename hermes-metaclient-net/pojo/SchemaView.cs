using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace com.ctrip.hermes.meta.client.pojo
{
    public class SchemaView
    {
        public long id { get; set; }

        public string name { get; set; }

        public string type { get; set; }

        public int version { get; set; }

        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime? createTime { get; set; }

        public string compatibility { get; set; }

        public string description { get; set; }

        public List<Property> properties { get; set; }

        public string schemaPreview { get; set; }

        public long? topicId { get; set; }
    }
}
