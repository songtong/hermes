package com.ctrip.hermes.monitor.checker.client;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.springframework.beans.factory.InitializingBean;
import org.unidal.tuple.Pair;
import org.xml.sax.SAXException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.admin.core.monitor.event.ProduceFailureCountTooLargeEvent;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.monitor.checker.CatBasedChecker;
import com.ctrip.hermes.monitor.checker.CheckerResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
//@Component(value = "ProduceFailureChecker")
public class ProduceFailureChecker extends CatBasedChecker implements InitializingBean {

	private static final String CAT_DOMAIN = "hermes";

	private List<String> m_excludedTopics = new LinkedList<>();

	@Override
	public void afterPropertiesSet() throws Exception {
		String excludedTopicsStr = m_config.getProduceFailureCheckerExcludedTopics();
		if (!StringUtils.isBlank(excludedTopicsStr)) {
			List<String> configedExcludedTopics = JSON.parseObject(excludedTopicsStr, new TypeReference<List<String>>() {
			});
			if (configedExcludedTopics != null) {
				m_excludedTopics.addAll(configedExcludedTopics);
			}
		}
	}

	@Override
	protected void doCheck(Timespan timespan, CheckerResult result) throws Exception {
//		String catReportUrl = m_config.getCatBaseUrl()
//		      + String.format(m_config.getCatEventUrlPattern(), CAT_DOMAIN, formatToCatUrlTime(timespan.getStartHour()),
//		            CatConstants.type_produce);
//		String transactionReportXml = curl(catReportUrl, m_config.getCatConnectTimeout(), m_config.getCatReadTimeout());
//		Map<String, List<Pair<Integer, Integer>>> topic2FailureCountList = extractFailureCountFromXml(transactionReportXml);
//		bizCheck(topic2FailureCountList, timespan, result);
	}

	private void bizCheck(Map<String, List<Pair<Integer, Integer>>> topic2FailureCountList, Timespan timespan,
	      CheckerResult result) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		for (Map.Entry<String, List<Pair<Integer, Integer>>> entry : topic2FailureCountList.entrySet()) {
			String topic = entry.getKey();
			List<Pair<Integer, Integer>> failureCountList = entry.getValue();

			if (!m_excludedTopics.contains(topic)) {
				for (Pair<Integer, Integer> pair : failureCountList) {
					int minute = pair.getKey();
					int failureCount = pair.getValue();

					if (timespan.getMinutes().contains(minute) && failureCount >= m_config.getProduceFailureCountThreshold()) {
						ProduceFailureCountTooLargeEvent monitorEvent = new ProduceFailureCountTooLargeEvent();
						monitorEvent.setTopic(topic);
						monitorEvent.setFailureCount(failureCount);

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

	private Map<String, List<Pair<Integer, Integer>>> extractFailureCountFromXml(String xmlContent) throws SAXException,
	      IOException, ParserConfigurationException, XPathExpressionException {
		Map<String, List<Pair<Integer, Integer>>> topic2FailureCountList = new HashMap<>();
//
//		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
//		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
//		Document doc = documentBuilder.parse(new ByteArrayInputStream(xmlContent.getBytes()));
//
//		XPath xPath = XPathFactory.newInstance().newXPath();
//
//		String allTransactionsExpression = "/event/report[@domain='" + CAT_DOMAIN + "']/machine[@ip='All']/type[@id='"
//		      + CatConstants.TYPE_MESSAGE_PRODUCE_ERROR + "']/name";
//
//		NodeList transactionNodes = (NodeList) xPath.compile(allTransactionsExpression).evaluate(doc,
//		      XPathConstants.NODESET);
//
//		for (int i = 0; i < transactionNodes.getLength(); i++) {
//			Node transactionNode = transactionNodes.item(i);
//
//			String topic = transactionNode.getAttributes().getNamedItem("id").getNodeValue();
//			topic2FailureCountList.put(topic, new LinkedList<Pair<Integer, Integer>>());
//
//			String allRangesExpression = "range";
//
//			NodeList rangeNodes = (NodeList) xPath.compile(allRangesExpression).evaluate(transactionNode,
//			      XPathConstants.NODESET);
//
//			for (int j = 0; j < rangeNodes.getLength(); j++) {
//				Node rangeNode = rangeNodes.item(j);
//				String countStr = rangeNode.getAttributes().getNamedItem("count").getNodeValue();
//				String minuteStr = rangeNode.getAttributes().getNamedItem("value").getNodeValue();
//				topic2FailureCountList.get(topic).add(
//				      new Pair<Integer, Integer>(Integer.parseInt(minuteStr), Integer.parseInt(countStr)));
//			}
//		}
//
		return topic2FailureCountList;
	}

	@Override
	public String name() {
		return "ProduceFailureChecker";
	}

}
