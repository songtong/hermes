using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace com.ctrip.hermes.meta.client.pojo
{
    public class TopicView
    {
        public long? id { get; set; }

        public string name { get; set; }

        public string storageType { get; set; }

        public string description { get; set; }

        public string otherinfo { get; set; }

        public string status { get; set; }

        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime? createTime { get; set; }

        [JsonConverter(typeof(UnixDateTimeConverter))]
        public DateTime? lastModifiedTime { get; set; }

        public List<Partition> partitions { get; set; }

        public List<Property> properties { get; set; }

        public Storage storage { get; set; }

        public long? schemaId { get; set; }

        public SchemaView schema { get; set; }

        private string codecType { get; set; }

        private Codec codec { get; set; }
    }
}
