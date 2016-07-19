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
		List<String> bus = Arrays.asList("Default", "会奖", "信息安全", "包团定制", "周边游", "商业智能", "商旅", "团购", "国际业务", "地面", "基础业务", "天海邮轮", "小微金融", "度假", "待归类", "搜索",
				"攻略", "服务产品", "机票", "框架", "汽车票", "火车票", "爱玩", "用车", "系统", "网站运营", "营销", "财务", "购物", "通信技术中心", "邮轮", "酒会", "酒店", "金融平台", "金融支付", "金融服务", "风险控制",
				"高端旅游");

//		bus = Arrays.asList("酒店");

		List<String> versions = Arrays.asList("java-0.5.4", "java-0.6.5", "java-0.6.6", "java-0.6.7", "java-0.7.0", "java-0.7.1", "java-0.7.2", "java-0.7.2.1",
				"java-0.7.2.2", "java-0.7.2.3", "net-0.6.1", "net-0.7.1", "net-0.7.2.1", "net-0.7.2.2", "net-0.7.2.3");

//		versions = Arrays.asList("java-0.7.2.4");

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

								Set<String> produceTopics = domainToTopic(domain, "Message.Produce.Tried");
								Set<String> consumeTopics = domainToTopic(domain, "Message.Consume.Poll.Tried");
								consumeTopics.addAll(domainToTopic(domain, "Message.Consume.Collect.Tried"));
								consumeTopics.addAll(domainToTopic(domain, "Message.Consumed"));

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
			System.out.println(entry.getKey());
			System.out.println(entry.getValue());
		}
	}

	private static Set<String> domainToTopic(String domain, String txType) throws Exception {
		String urlFmt = "http://cat.ctripcorp.com/cat/r/t?op=graphs&domain=%s&date=%s&ip=All&type=%s&forceDownload=xml";
		String xml = fetchXml(String.format(urlFmt, domain, yesterday(), txType));

		Set<String> topics = new HashSet<>();

		if (xml != null) {
			DOMParser parser = new DOMParser();
			parser.parse(new InputSource(new StringReader(xml)));
			Document doc = parser.getDocument();

			NodeList names = doc.getElementsByTagName("name");
			for (int i = 0; i < names.getLength(); i++) {
				Node name = names.item(i);
				String idAttr = name.getAttributes().getNamedItem("id").getNodeValue();
				if (!"All".equals(idAttr)) {
					topics.add(idAttr);
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
		private Map<String, Pair<HashSet<String>, HashSet<String>>> domain2PC = new HashMap<>();

		public void addToDomain(String domain, Set<String> produceTopics, Set<String> consumeTopics) {
			if (!domain2PC.containsKey(domain)) {
				domain2PC.put(domain, new Pair<>(new HashSet<String>(), new HashSet<String>()));
			}

			Pair<HashSet<String>, HashSet<String>> pair = domain2PC.get(domain);

			pair.getKey().addAll(produceTopics);
			pair.getValue().addAll(consumeTopics);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();

			for (Entry<String, Pair<HashSet<String>, HashSet<String>>> entry : domain2PC.entrySet()) {
				sb.append(String.format("\t%s\n\t\tProduce: %s\n\t\tConsume: %s\n", entry.getKey(), entry.getValue().getKey(), entry.getValue().getValue()));
			}

			return sb.toString();
		}

	}

}
