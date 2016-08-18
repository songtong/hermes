/**
 * 
 */
package com.ctrip.hermes.example.cat;

import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.unidal.helper.Files.IO;
import org.unidal.tuple.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.sun.org.apache.xerces.internal.parsers.DOMParser;

/**
 * @author marsqing
 *
 *         Jul 14, 2016 6:20:42 PM
 */
public class CatVersionSpider {

	public static void main(String[] args) throws Exception {
		List<String> bus = Arrays.asList("会奖", "信息安全", "创新工场", "包团定制", "周边游", "商业智能", "商旅", "团购", "国际业务", "地面", "基础业务",
		      "天海邮轮", "小微金融", "度假", "待归类", "技术管理中心", "搜索", "攻略", "服务产品", "机票", "框架", "汽车票", "火车票", "爱玩", "用车", "系统",
		      "网站运营", "营销", "财务", "购物", "通信技术中心", "邮轮", "酒会", "酒店", "金融平台", "金融支付", "金融服务", "风险控制", "高端旅游");

		//bus = Arrays.asList("酒店", "框架");

		List<String> versions = Arrays.asList("java-0.6.5", "java-0.7.0", "java-0.7.1", "java-0.7.2.1", "java-0.7.2.4",
		      "java-0.7.2.5", "net-0.7.2.2", "net-0.7.2.6", "net-0.7.2.6.1");

		//versions = Arrays.asList("java-0.7.2.4", "java-0.7.1");

		String urlFmt = "http://cat.ctripcorp.com/cat/r/e?op=historyGraph&domain=All&date=%s&ip=%s&reportType=day&type=Hermes.Client.Version&name=%s&startDate=%s&endDate=%s&forceDownload=xml";
		String startDate = yesterday();
		String endDate = today();

		Map<String, VersionInfo> version2VersionInfo = new HashMap<>();

		for (String bu : bus) {
			System.out.println("BU: " + bu);
			for (String hermesVersion : versions) {
				System.out.println("Version: " + hermesVersion);
				String url = String.format(urlFmt, startDate, bu, hermesVersion, startDate, endDate);
				String xml = null;
				try {
					xml = fetchXml(url);
				} catch (Exception e) {
					Thread.sleep(100);
					continue;
				}

				if (xml != null) {
					Thread.sleep(100);

					DOMParser parser = new DOMParser();
					parser.parse(new InputSource(new StringReader(xml)));
					Document doc = parser.getDocument();

					NodeList nameDomains = doc.getElementsByTagName("name-domain");
					for (int i = 0; i < nameDomains.getLength(); i++) {
						Node nameDomainEle = nameDomains.item(i);
						Node nameAttr = nameDomainEle.getAttributes().getNamedItem("name");
						String version = nameAttr.getNodeValue();

						if (!version2VersionInfo.containsKey(version)) {
							version2VersionInfo.put(version, new VersionInfo());
						}

						NodeList children = nameDomainEle.getChildNodes();
						for (int j = 0; j < children.getLength(); j++) {
							Node child = children.item(j);
							if ("name-domain-count".equals(child.getLocalName())) {
								Node domainNode = child.getAttributes().getNamedItem("domain");
								String domain = domainNode.getNodeValue();

								Map<String, Long> produceTopics = domainToTopic(domain, "Message.Produce.Tried");
								Map<String, Long> consumeTopics = domainToTopic(domain, "Message.Consumed");
								if (consumeTopics.isEmpty()) {
									consumeTopics = domainToTopic(domain, "Message.Consume.Poll.Tried");
									if (consumeTopics.isEmpty()) {
										consumeTopics = domainToTopic(domain, "Message.Consume.Collect.Tried");
									}
								}
								// consumeTopics.addAll(domainToTopic(domain, "Message.Consume.Collect.Tried"));
								// consumeTopics.addAll(domainToTopic(domain, "Message.Consumed"));

								VersionInfo versionInfo = version2VersionInfo.get(version);
								versionInfo.addToDomain(domain, produceTopics, consumeTopics);
							}
						}
					}
				}
			}
		}

		System.out.println();
		for (Entry<String, VersionInfo> entry : version2VersionInfo.entrySet()) {
			System.out.println(String.format("%s\tproducer:%s flow:%s\tconsumer:%s flow:%s", entry.getKey(), entry
			      .getValue().getProducerCount(), entry.getValue().getProducerFlow(), entry.getValue().getConsumerCount(),
			      entry.getValue().getConsumerFlow()));
			//System.out.println(entry.getValue());
		}
	}

	private static Map<String, Long> domainToTopic(String domain, String txType) throws Exception {
		String urlFmt = "http://cat.ctripcorp.com/cat/r/t?op=history&domain=%s&date=%s&ip=All&type=%s&forceDownload=xml";
		String xml = fetchXml(String.format(urlFmt, domain, yesterday(), txType));

		Map<String, Long> topics = new HashMap<>();

		if (xml != null) {
			DOMParser parser = new DOMParser();
			parser.parse(new InputSource(new StringReader(xml)));
			Document doc = parser.getDocument();

			NodeList names = doc.getElementsByTagName("name");
			for (int i = 0; i < names.getLength(); i++) {
				Node name = names.item(i);
				String idAttr = name.getAttributes().getNamedItem("id").getNodeValue();
				Long totalCount = Long.valueOf(name.getAttributes().getNamedItem("totalCount").getNodeValue());
				if (!"All".equals(idAttr)) {
					topics.put(idAttr, totalCount);
				}
			}
		}

		return topics;
	}

	private static String fetchXml(String url) {
		long start = System.currentTimeMillis();
		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();

			conn.setConnectTimeout(3000);
			conn.setReadTimeout(10000);

			int code = conn.getResponseCode();

			if (code == 200) {
				return IO.INSTANCE.readFrom(conn.getInputStream(), "utf-8");
			} else {
				return null;
			}
		} catch (Exception e) {
			System.out.println(String.format("Got nothing from %s", url));
			return null;
		} finally {
			System.out.println(String.format("Elapse %s when fetch %s", System.currentTimeMillis() - start, url));
		}
	}

	private static String yesterday() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DAY_OF_MONTH, -1);
		return sdf.format(c.getTime());
	}

	private static String today() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		return sdf.format(new Date());
	}

	static class VersionInfo {
		// domain => (Producer, Consumer)
		private Map<String, Pair<Map<String, Long>, Map<String, Long>>> domain2PC = new HashMap<>();

		private int producerCount = 0;

		private long producerFlow = 0;

		private int consumerCount = 0;

		private long consumerFlow = 0;

		public void addToDomain(String domain, Map<String, Long> produceTopics, Map<String, Long> consumeTopics) {
			if (!domain2PC.containsKey(domain)) {
				domain2PC.put(domain, new Pair<Map<String, Long>, Map<String, Long>>());
				Pair<Map<String, Long>, Map<String, Long>> pair = domain2PC.get(domain);
				pair.setKey(produceTopics);
				pair.setValue(consumeTopics);

				producerCount += produceTopics.size();
				for (Entry<String, Long> topic : produceTopics.entrySet()) {
					producerFlow += topic.getValue();
				}

				consumerCount += consumeTopics.size();
				for (Entry<String, Long> topic : consumeTopics.entrySet()) {
					consumerFlow += topic.getValue();
				}
			}

		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();

			for (Entry<String, Pair<Map<String, Long>, Map<String, Long>>> entry : domain2PC.entrySet()) {
				sb.append(String.format("\t%s\n", entry.getKey()));
				for (Entry<String, Long> topics : entry.getValue().getKey().entrySet()) {
					sb.append(String.format("\t\tProduce: %s\t%s\n", topics.getKey(), topics.getValue()));
				}
				for (Entry<String, Long> topics : entry.getValue().getValue().entrySet()) {
					sb.append(String.format("\t\tConsume: %s\t%s\n", topics.getKey(), topics.getValue()));
				}
			}
			return sb.toString();
		}

		public Map<String, Pair<Map<String, Long>, Map<String, Long>>> getDomain2PC() {
			return domain2PC;
		}

		public void setDomain2PC(Map<String, Pair<Map<String, Long>, Map<String, Long>>> domain2pc) {
			domain2PC = domain2pc;
		}

		public int getProducerCount() {
			return producerCount;
		}

		public void setProducerCount(int producerCount) {
			this.producerCount = producerCount;
		}

		public long getProducerFlow() {
			return producerFlow;
		}

		public void setProducerFlow(long producerFlow) {
			this.producerFlow = producerFlow;
		}

		public int getConsumerCount() {
			return consumerCount;
		}

		public void setConsumerCount(int consumerCount) {
			this.consumerCount = consumerCount;
		}

		public long getConsumerFlow() {
			return consumerFlow;
		}

		public void setConsumerFlow(long consumerFlow) {
			this.consumerFlow = consumerFlow;
		}

	}

}
