using System.Collections.Generic;
using System.Runtime.Serialization;
using System;
using Newtonsoft.Json;

namespace com.ctrip.hermes.meta.client.pojo
{
    public class Topic
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

        public int? partitionCount { get; set; }

        public List<Partition> partitions { get; set; }

        public List<Property> properties { get; set; }

        public Storage storage { get; set; }

        public long? schemaId { get; set; }

        public SchemaView schemaView { get; set; }

        private string codecType { get; set; }

        private Codec codec { get; set; }

        public string consumerRetryPolicy { get; set; }

        public List<ConsumerGroup> consumerGroups { get; set; }

        public List<Producer> producers { get; set; }
    }
}
