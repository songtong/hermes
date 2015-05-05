using System.Net;
using System.Runtime.Serialization.Json;
using com.ctrip.hermes.meta.client.pojo;
using System.IO;
using Newtonsoft.Json;

namespace com.ctrip.hermes.meta.client.rest
{
    public class MetaClient
    {
        public Meta getMeta()
        {
            var RestConfig = new RestConfig();
            HttpWebRequest request = WebRequest.Create(RestConfig.meta) as HttpWebRequest;
            request.Method = "GET";
            HttpWebResponse response = request.GetResponse() as HttpWebResponse;
            Stream responseStream = response.GetResponseStream();
            string content = new StreamReader(responseStream).ReadToEnd();
            responseStream.Close();
            Meta meta = (Meta)JsonConvert.DeserializeObject(content, typeof(Meta));
            return meta;
        }
    }
}
