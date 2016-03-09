package com.ctrip.hermes.monitor.checker.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.monitor.event.ProduceTransportFailedRatioErrorEvent;
import com.ctrip.hermes.monitor.checker.CatBasedChecker;
import com.ctrip.hermes.monitor.checker.CheckerResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Component(value = "ProduceTransportFailedRatioChecker")
public class ProduceTransportFailedRatioChecker extends CatBasedChecker implements InitializingBean {

	private List<String> m_excludedTopics = new LinkedList<>();

	@Override
	public void afterPropertiesSet() throws Exception {
		String excludedTopicsStr = m_config.getProduceTransportFailedRatioCheckerExcludedTopics();
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
		Meta meta = fetchMeta();
		bizCheck(getTransportCountListData(timespan), timespan, meta, result);
	}

	private void bizCheck(Map<String, Map<Integer, Pair<Integer, Integer>>> topic2SendCmdCount, Timespan timespan, Meta meta,
	      CheckerResult result) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(timespan.getStartHour());
		calendar.set(Calendar.MINUTE, Collections.max(timespan.getMinutes()));
		calendar.set(Calendar.SECOND, 59);
		Date toDate = calendar.getTime();
		calendar.set(Calendar.MINUTE, Collections.min(timespan.getMinutes()));
		calendar.set(Calendar.SECOND, 0);
		Date fromDate = calendar.getTime();

		for (Topic topic : meta.getTopics().values()) {
			String topicName = topic.getName();
			if (Storage.MYSQL.equals(topic.getStorageType()) && !m_excludedTopics.contains(topicName)) {
				Pair<Integer, Integer> sendCmdCountSum = sumCount(topic2SendCmdCount.get(topicName), timespan);

				boolean shouldAlert = false;
				if (sendCmdCountSum.getValue() > 0
				      && Math.abs(sendCmdCountSum.getValue()) / (double) sendCmdCountSum.getKey() > m_config
				            .getProduceTransportFailedRatioThreshold()) {
					shouldAlert = true;
				}

				if (shouldAlert) {
					ProduceTransportFailedRatioErrorEvent monitorEvent = new ProduceTransportFailedRatioErrorEvent();
					monitorEvent.setTopic(topicName);
					monitorEvent.setTotal(sendCmdCountSum.getKey());
					monitorEvent.setFailed(sendCmdCountSum.getValue());
					monitorEvent.setTimespan(sdf.format(fromDate) + " ~ " + sdf.format(toDate));
					result.addMonitorEvent(monitorEvent);
				}
			}
		}

		result.setRunSuccess(true);
	}

	private Pair<Integer, Integer> sumCount(Map<Integer, Pair<Integer, Integer>> map, Timespan timespan) {
		int totalSum = 0;
		int failsSum = 0;
		if (map != null) {
			for (Entry<Integer, Pair<Integer, Integer>> minuteCountPair : map.entrySet()) {
				int minute = minuteCountPair.getKey();
				int totalCount = minuteCountPair.getValue().getKey();
				int failsCount = minuteCountPair.getValue().getValue();
				if (timespan.getMinutes().contains(minute)) {
					totalSum += totalCount;
					failsSum += failsCount;
				}
			}
		}
		return new Pair<Integer, Integer>(totalSum, failsSum);
	}

	private Map<String, Map<Integer, Pair<Integer, Integer>>> getTransportCountListData(Timespan timespan) throws IOException,
	      XPathExpressionException, SAXException, ParserConfigurationException {
		String transactionReportXml = getTransactionReportFromCat(timespan, CatConstants.TYPE_MESSAGE_PRODUCE_TRANSPORT);
		return extractDatasFromXml(transactionReportXml, CatConstants.TYPE_MESSAGE_PRODUCE_TRANSPORT);
	}

	protected String getTransactionReportFromCat(Timespan timespan, String transactionType) throws IOException {
		String catReportUrl = m_config.getCatBaseUrl()
		      + String.format(m_config.getCatCrossTransactionUrlPattern(), formatToCatUrlTime(timespan.getStartHour()),
		            transactionType);
		String transactionReportXml = curl(catReportUrl, m_config.getCatConnectTimeout(), m_config.getCatReadTimeout());
		return transactionReportXml;
	}

	private Map<String, Map<Integer, Pair<Integer, Integer>>> extractDatasFromXml(String xmlContent, String transactionType)
	      throws SAXException, IOException, ParserConfigurationException, XPathExpressionException {
		Map<String, Map<Integer, Pair<Integer, Integer>>> topic2CountList = new HashMap<>();

		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document doc = documentBuilder.parse(new ByteArrayInputStream(xmlContent.getBytes()));

		XPath xPath = XPathFactory.newInstance().newXPath();

		String allTransactionsExpression = "/transaction/report[@domain='All']/machine[@ip='All']/type[@id='" + transactionType
		      + "']/name";

		NodeList transactionNodes = (NodeList) xPath.compile(allTransactionsExpression).evaluate(doc, XPathConstants.NODESET);

		for (int i = 0; i < transactionNodes.getLength(); i++) {
			Node transactionNode = transactionNodes.item(i);

			String topic = transactionNode.getAttributes().getNamedItem("id").getNodeValue();
			topic2CountList.put(topic, new LinkedHashMap<Integer, Pair<Integer, Integer>>());

			String allRangesExpression = "range";

			NodeList rangeNodes = (NodeList) xPath.compile(allRangesExpression).evaluate(transactionNode, XPathConstants.NODESET);

			for (int j = 0; j < rangeNodes.getLength(); j++) {
				Node rangeNode = rangeNodes.item(j);
				String countStr = rangeNode.getAttributes().getNamedItem("count").getNodeValue();
				String failsStr = rangeNode.getAttributes().getNamedItem("fails").getNodeValue();
				String minuteStr = rangeNode.getAttributes().getNamedItem("value").getNodeValue();
				topic2CountList.get(topic).put(Integer.parseInt(minuteStr),
				      new Pair<Integer, Integer>(Integer.parseInt(countStr), Integer.parseInt(failsStr)));
			}
		}

		return topic2CountList;
	}

	protected Meta fetchMeta() throws IOException, SAXException {
		String metaStr = curl(m_config.getMetaRestUrl(), 3000, 1000);
		return JSON.parseObject(metaStr, Meta.class);
	}

	@Override
	public String name() {
		return "ProduceTransportFailedRatioChecker";
	}
}
