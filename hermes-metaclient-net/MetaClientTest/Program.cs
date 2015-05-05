using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.ctrip.hermes.meta.client.rest;
using com.ctrip.hermes.meta.client.pojo;

namespace MetaClientTest
{
    class Program
    {
        static void Main(string[] args)
        {
            TopicClient topicClient = new TopicClient();
            List<TopicView> topics = topicClient.getTopics();
            Console.WriteLine(topics.Count);

            MetaClient metaClient = new MetaClient();
            Meta meta = metaClient.getMeta();
            Console.WriteLine(meta.version);
            Console.Read();
        }
    }
}
