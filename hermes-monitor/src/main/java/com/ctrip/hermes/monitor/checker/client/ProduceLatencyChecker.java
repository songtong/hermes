package com.ctrip.hermes.monitor.checker.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.monitor.event.ProduceLatencyTooLargeEvent;
import com.ctrip.hermes.monitor.checker.CatBasedChecker;
import com.ctrip.hermes.monitor.checker.CheckerResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Component(value = "ProducerLatencyChecker")
public class ProduceLatencyChecker extends CatBasedChecker implements InitializingBean {

	private static final String DEFAULT_THRESHOLD_KEY = "Default";

	private List<String> m_excludedTopics = new LinkedList<>();

	private Map<String, Integer> m_thresholds = new HashMap<>();

	@Override
	public void afterPropertiesSet() throws Exception {
		String excludedTopicsStr = m_config.getProduceLatencyCheckerExcludedTopics();
		if (!StringUtils.isBlank(excludedTopicsStr)) {
			List<String> configedExcludedTopics = JSON.parseObject(excludedTopicsStr, new TypeReference<List<String>>() {
			});
			if (configedExcludedTopics != null) {
				m_excludedTopics.addAll(configedExcludedTopics);
			}
		}

		String thresholdsStr = m_config.getProduceLatencyThresholds();
		if (!StringUtils.isBlank(thresholdsStr)) {
			Map<String, Integer> configedThresholds = JSON.parseObject(thresholdsStr,
			      new TypeReference<Map<String, Integer>>() {
			      });
			if (configedThresholds != null) {
				m_thresholds.putAll(configedThresholds);
			}
		}
	}

	@Override
	protected void doCheck(Timespan timespan, CheckerResult result) throws Exception {
		String catReportUrl = m_config.getCatBaseUrl()
		      + String.format(m_config.getCatCrossTransactionUrlPattern(), formatToCatUrlTime(timespan.getStartHour()),
		            CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE);
		String transactionReportXml = curl(catReportUrl, m_config.getCatConnectTimeout(), m_config.getCatReadTimeout());
		Map<String, List<Pair<Integer, Double>>> topic2LatencyList = extractLatencyDatasFromXml(transactionReportXml);
		bizCheck(topic2LatencyList, timespan, result);
	}

	private void bizCheck(Map<String, List<Pair<Integer, Double>>> topic2LatencyList, Timespan timespan,
	      CheckerResult result) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		for (Map.Entry<String, List<Pair<Integer, Double>>> entry : topic2LatencyList.entrySet()) {
			String topic = entry.getKey();
			List<Pair<Integer, Double>> latencyList = entry.getValue();

			if (!m_excludedTopics.contains(topic)) {
				for (Pair<Integer, Double> pair : latencyList) {
					int minute = pair.getKey();
					double latency = pair.getValue();

					int threshold = m_thresholds.containsKey(topic) ? m_thresholds.get(topic) : (m_thresholds
					      .containsKey(DEFAULT_THRESHOLD_KEY) ? m_thresholds.get(DEFAULT_THRESHOLD_KEY) : Integer.MAX_VALUE);

					if (timespan.getMinutes().contains(minute) && latency > threshold) {
						ProduceLatencyTooLargeEvent monitorEvent = new ProduceLatencyTooLargeEvent();
						monitorEvent.setTopic(topic);
						monitorEvent.setLatency(latency);

						Calendar calendar = Calendar.getInstance();
						calendar.setTime(timespan.getStartHour());
						calendar.set(Calendar.MINUTE, minute);
						calendar.set(Calendar.SECOND, 0);

						monitorEvent.setDate(sdf.format(calendar.getTime()));
						result.addMonitorEvent(monitorEvent);
					}
				}
			}
		}

		result.setRunSuccess(true);
	}

	private Map<String, List<Pair<Integer, Double>>> extractLatencyDatasFromXml(String xmlContent) throws SAXException,
	      IOException, ParserConfigurationException, XPathExpressionException {
		Map<String, List<Pair<Integer, Double>>> topic2LatencyList = new HashMap<>();

		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document doc = documentBuilder.parse(new ByteArrayInputStream(xmlContent.getBytes()));

		XPath xPath = XPathFactory.newInstance().newXPath();

		String allTransactionsExpression = "/transaction/report[@domain='All']/machine[@ip='All']/type[@id='"
		      + CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE + "']/name";

		NodeList transactionNodes = (NodeList) xPath.compile(allTransactionsExpression).evaluate(doc,
		      XPathConstants.NODESET);

		for (int i = 0; i < transactionNodes.getLength(); i++) {
			Node transactionNode = transactionNodes.item(i);

			String topic = transactionNode.getAttributes().getNamedItem("id").getNodeValue();
			topic2LatencyList.put(topic, new LinkedList<Pair<Integer, Double>>());

			String allRangesExpression = "range";

			NodeList rangeNodes = (NodeList) xPath.compile(allRangesExpression).evaluate(transactionNode,
			      XPathConstants.NODESET);

			for (int j = 0; j < rangeNodes.getLength(); j++) {
				Node rangeNode = rangeNodes.item(j);
				String avgStr = rangeNode.getAttributes().getNamedItem("avg").getNodeValue();
				String minuteStr = rangeNode.getAttributes().getNamedItem("value").getNodeValue();
				topic2LatencyList.get(topic).add(
				      new Pair<Integer, Double>(Integer.parseInt(minuteStr), Double.parseDouble(avgStr)));
			}
		}

		return topic2LatencyList;
	}

	@Override
	public String name() {
		return "ProducerLatencyChecker";
	}
}
