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
import com.ctrip.hermes.admin.core.monitor.event.ConsumeDelayTooLargeEvent;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.monitor.checker.CatBasedChecker;
import com.ctrip.hermes.monitor.checker.CheckerResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
//@Component(value = "ConsumeDelayChecker")
public class ConsumeDelayChecker extends CatBasedChecker implements InitializingBean {

	// key: topic, value: threshold in millisecond
	private Map<String, Double> m_thresholds = new HashMap<>();

	@Override
	public void afterPropertiesSet() throws Exception {
		String consumeDelayThresholds = m_config.getConsumeDelayThresholds();

		if (!StringUtils.isBlank(consumeDelayThresholds)) {
			Map<String, Double> configedThresholds = JSON.parseObject(consumeDelayThresholds,
			      new TypeReference<Map<String, Double>>() {
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
		            CatConstants.TYPE_MESSAGE_CONSUME_LATENCY);
		String transactionReportXml = curl(catReportUrl, m_config.getCatConnectTimeout(), m_config.getCatReadTimeout());
		Map<String, List<Pair<Integer, Double>>> topicConsumerGroup2DelayList = extractDelayDatasFromXml(transactionReportXml);
		bizCheck(topicConsumerGroup2DelayList, timespan, result);
	}

	private void bizCheck(Map<String, List<Pair<Integer, Double>>> topicConsumerGroup2DelayList, Timespan timespan,
	      CheckerResult result) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		for (Map.Entry<String, List<Pair<Integer, Double>>> entry : topicConsumerGroup2DelayList.entrySet()) {
			String topicConsumerGroup = entry.getKey();
			List<Pair<Integer, Double>> delayList = entry.getValue();

			if (m_thresholds.containsKey(topicConsumerGroup)) {
				for (Pair<Integer, Double> pair : delayList) {
					int minute = pair.getKey();
					double delay = pair.getValue();

					Double threshold = m_thresholds.get(topicConsumerGroup);

					if (timespan.getMinutes().contains(minute) && delay > threshold) {
						ConsumeDelayTooLargeEvent monitorEvent = new ConsumeDelayTooLargeEvent();
						String[] splits = topicConsumerGroup.split(":");

						if (splits != null && splits.length == 2) {
							monitorEvent.setTopic(splits[0]);
							monitorEvent.setConsumerGroup(splits[1]);
							monitorEvent.setDelay(delay);

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
		}

		result.setRunSuccess(true);
	}

	private Map<String, List<Pair<Integer, Double>>> extractDelayDatasFromXml(String xmlContent) throws SAXException,
	      IOException, ParserConfigurationException, XPathExpressionException {
		Map<String, List<Pair<Integer, Double>>> topic2DelayList = new HashMap<>();

		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document doc = documentBuilder.parse(new ByteArrayInputStream(xmlContent.getBytes()));

		XPath xPath = XPathFactory.newInstance().newXPath();

		String allTransactionsExpression = "/transaction/report[@domain='All']/machine[@ip='All']/type[@id='"
		      + CatConstants.TYPE_MESSAGE_CONSUME_LATENCY + "']/name";

		NodeList transactionNodes = (NodeList) xPath.compile(allTransactionsExpression).evaluate(doc,
		      XPathConstants.NODESET);

		for (int i = 0; i < transactionNodes.getLength(); i++) {
			Node transactionNode = transactionNodes.item(i);

			String topic = transactionNode.getAttributes().getNamedItem("id").getNodeValue();
			topic2DelayList.put(topic, new LinkedList<Pair<Integer, Double>>());

			String allRangesExpression = "range";

			NodeList rangeNodes = (NodeList) xPath.compile(allRangesExpression).evaluate(transactionNode,
			      XPathConstants.NODESET);

			for (int j = 0; j < rangeNodes.getLength(); j++) {
				Node rangeNode = rangeNodes.item(j);
				String avgStr = rangeNode.getAttributes().getNamedItem("avg").getNodeValue();
				String minuteStr = rangeNode.getAttributes().getNamedItem("value").getNodeValue();
				topic2DelayList.get(topic).add(
				      new Pair<Integer, Double>(Integer.parseInt(minuteStr), Double.parseDouble(avgStr)));
			}
		}

		return topic2DelayList;
	}

	@Override
	public String name() {
		return "ConsumeDelayChecker";
	}

}
