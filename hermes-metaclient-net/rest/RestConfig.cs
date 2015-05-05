
using System.Configuration;
namespace com.ctrip.hermes.meta.client.rest
{
    class RestConfig
    {
        public string baseurl
        {
            get
            {
                string metahost = ConfigurationManager.AppSettings["meta-host"];
                string metaport = ConfigurationManager.AppSettings["meta-port"];
                return "http://" + metahost + ":" + metaport;
            }
        }

        public string meta { get { return baseurl + "/meta/"; } }

        public string topics { get { return baseurl + "/topics/"; } }

        public string schemas { get { return baseurl + "/schemas/"; } }

        public string codecs { get { return baseurl + "/codecs/"; } }
    }
}
