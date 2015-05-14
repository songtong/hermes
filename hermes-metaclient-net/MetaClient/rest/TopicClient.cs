using System.Collections.Generic;
using System.IO;
using System.Net;
using com.ctrip.hermes.meta.client.pojo;
using Newtonsoft.Json;

namespace com.ctrip.hermes.meta.client.rest
{
    public class TopicClient
    {
        public List<TopicView> getTopics()
        {
            var RestConfig = new RestConfig();
            HttpWebRequest request = WebRequest.Create(RestConfig.topics) as HttpWebRequest;
            request.Method = "GET";
            HttpWebResponse response = request.GetResponse() as HttpWebResponse;
            Stream responseStream = response.GetResponseStream();
            string content = new StreamReader(responseStream).ReadToEnd();
            responseStream.Close();
            List<TopicView> topics = (List<TopicView>)JsonConvert.DeserializeObject(content, typeof(List<TopicView>));
            return topics;
        }

        public TopicView getTopic(string name)
        {
            var RestConfig = new RestConfig();
            HttpWebRequest request = WebRequest.Create(RestConfig.topics + name) as HttpWebRequest;
            request.Method = "GET";
            HttpWebResponse response = request.GetResponse() as HttpWebResponse;
            Stream responseStream = response.GetResponseStream();
            string content = new StreamReader(responseStream).ReadToEnd();
            responseStream.Close();
            TopicView topic = (TopicView)JsonConvert.DeserializeObject(content, typeof(TopicView));
            return topic;
        }
    }
}
