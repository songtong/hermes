using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using com.ctrip.hermes.meta.client.rest;
using com.ctrip.hermes.meta.client.pojo;

namespace KafkaClientTest
{
    class Program
    {
        static List<Uri> getBrokerList(TopicView topicView)
        {
            List<Uri> result = new List<Uri>();
            List<com.ctrip.hermes.meta.client.pojo.Partition> partitions = topicView.partitions;
            if (partitions == null || partitions.Count < 1)
                return result;
            string producerDataSource = partitions[0].writeDatasource;

            Storage producerStorage = topicView.storage;
            if (producerStorage == null)
                return result;

            foreach (Datasource datasource in producerStorage.datasources)
            {
                if (producerDataSource.Equals(datasource.id))
                {
                    Dictionary<string, Property> properties = datasource.properties;
                    foreach (var property in properties)
                    {
                        if (property.Key.Equals("bootstrap.servers"))
                        {
                            string[] uris = property.Value.value.Split(',');
                            foreach (var uri in uris)
                            {
                                result.Add(new Uri("http://" + uri));
                            }
                            break;
                        }
                    }
                }
            }

            return result;
        }

        static void Main(string[] args)
        {
            const string topicName = "kafka.SimpleTopic";
            TopicClient topicClient = new TopicClient();
            TopicView topicView = topicClient.getTopic(topicName);
            List<Uri> brokers = getBrokerList(topicView);
            //create an options file that sets up driver preferences
            var options = new KafkaOptions()
            {
                Log = new ConsoleLog()
            };
            options.KafkaServerUri = brokers;

            //start an out of process thread that runs a consumer that will write all received messages to the console
            Task.Run(() =>
            {
                var consumer = new Consumer(new ConsumerOptions(topicName, new BrokerRouter(options)) { Log = new ConsoleLog() });
                foreach (var data in consumer.Consume())
                {
                    Console.WriteLine("Response: P{0},O{1} : {2}", data.Meta.PartitionId, data.Meta.Offset, data.Value.ToUtf8String());
                }
            });

            //create a producer to send messages with
            var producer = new KafkaNet.Producer(new BrokerRouter(options))
            {
                BatchSize = 100,
                BatchDelayTime = TimeSpan.FromMilliseconds(2000)
            };


            //take in console read messages
            Console.WriteLine("Type a message and press enter...");
            while (true)
            {
                var message = Console.ReadLine();
                if (message == "quit") break;

                if (string.IsNullOrEmpty(message))
                {
                    //send a random batch of messages
                    SendRandomBatch(producer, topicName, 200);
                }
                else
                {
                    producer.SendMessageAsync(topicName, new[] { new Message(message) });
                }
            }

            using (producer)
            {

            }
        }

        private static async void SendRandomBatch(KafkaNet.Producer producer, string topicName, int count)
        {
            //send multiple messages
            var sendTask = producer.SendMessageAsync(topicName, Enumerable.Range(0, count).Select(x => new Message(x.ToString())));

            Console.WriteLine("Posted #{0} messages.  Buffered:{1} AsyncCount:{2}", count, producer.BufferCount, producer.AsyncCount);

            var response = await sendTask;

            Console.WriteLine("Completed send of batch: {0}. Buffered:{1} AsyncCount:{2}", count, producer.BufferCount, producer.AsyncCount);
            foreach (var result in response.OrderBy(x => x.PartitionId))
            {
                Console.WriteLine("Topic:{0} PartitionId:{1} Offset:{2}", result.Topic, result.PartitionId, result.Offset);
            }

        }
    }
}
