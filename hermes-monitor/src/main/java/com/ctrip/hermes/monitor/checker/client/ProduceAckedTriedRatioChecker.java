package com.ctrip.hermes.monitor.checker.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
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
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.monitor.event.ProduceAckedTriedRatioErrorEvent;
import com.ctrip.hermes.monitor.checker.CheckerResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Component(value = "ProduceAckedTriedRatioChecker")
public class ProduceAckedTriedRatioChecker extends CatBasedChecker implements InitializingBean {

	private List<String> m_excludedTopics = new LinkedList<>();

	@Override
	public void afterPropertiesSet() throws Exception {
		String excludedTopicsStr = m_config.getProduceAckedTriedRatioCheckerExcludedTopics();
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

		Map<String, List<Pair<Integer, Integer>>> topic2TriedCount = getTriedCountListData(timespan);
		Map<String, List<Pair<Integer, Integer>>> topic2AckedCount = getAckedCountListData(timespan);

		bizCheck(topic2TriedCount, topic2AckedCount, timespan, meta, result);
	}

	private void bizCheck(Map<String, List<Pair<Integer, Integer>>> topic2TriedCount,
	      Map<String, List<Pair<Integer, Integer>>> topic2AckedCount, Timespan timespan, Meta meta, CheckerResult result) {

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
				int triedCountSum = sumCount(topic2TriedCount.get(topicName), timespan);
				int ackedCountSum = sumCount(topic2AckedCount.get(topicName), timespan);

				boolean shouldAlert = false;
				if (triedCountSum == 0) {
					if (ackedCountSum > m_config.getProduceAckedTriedRatioAckedCountThresholdWhileNoProduceTried()) {
						shouldAlert = true;
					}
				} else {
					if (triedCountSum != ackedCountSum
					      && Math.abs(triedCountSum - ackedCountSum) / (double) triedCountSum > m_config
					            .getProduceAckedTriedRatioThreshold()) {
						shouldAlert = true;
					}
				}

				if (shouldAlert) {
					ProduceAckedTriedRatioErrorEvent monitorEvent = new ProduceAckedTriedRatioErrorEvent();
					monitorEvent.setTopic(topicName);
					monitorEvent.setTried(triedCountSum);
					monitorEvent.setAcked(ackedCountSum);
					monitorEvent.setTimespan(sdf.format(fromDate) + " ~ " + sdf.format(toDate));
					result.addMonitorEvent(monitorEvent);
				}
			}
		}

		result.setRunSuccess(true);
	}

	private int sumCount(List<Pair<Integer, Integer>> minuteCountPairList, Timespan timespan) {
		int sum = 0;
		if (minuteCountPairList != null) {
			for (Pair<Integer, Integer> minuteCountPair : minuteCountPairList) {
				int minute = minuteCountPair.getKey();
				int count = minuteCountPair.getValue();
				if (timespan.getMinutes().contains(minute)) {
					sum += count;
				}
			}
		}
		return sum;
	}

	private Map<String, List<Pair<Integer, Integer>>> getAckedCountListData(Timespan timespan) throws IOException,
	      XPathExpressionException, SAXException, ParserConfigurationException {
		return getTopic2CountListFromCat(timespan, "Message.Produce.Acked");
	}

	private Map<String, List<Pair<Integer, Integer>>> getTriedCountListData(Timespan timespan) throws IOException,
	      XPathExpressionException, SAXException, ParserConfigurationException {
		return getTopic2CountListFromCat(timespan, "Message.Produce.Tried");
	}

	protected Map<String, List<Pair<Integer, Integer>>> getTopic2CountListFromCat(Timespan timespan,
	      String transactionType) throws IOException, XPathExpressionException, SAXException,
	      ParserConfigurationException {
		String transactionReportXml = getTransactionReportFromCat(timespan, transactionType);

		return extractDatasFromXml(transactionReportXml, transactionType);
	}

	protected String getTransactionReportFromCat(Timespan timespan, String transactionType) throws IOException {
		String catReportUrl = m_config.getCatBaseUrl()
		      + String.format(m_config.getCatCrossTransactionUrlPattern(), formatToCatUrlTime(timespan.getStartHour()),
		            transactionType);
		String transactionReportXml = curl(catReportUrl, m_config.getCatConnectTimeout(), m_config.getCatReadTimeout());
		return transactionReportXml;
	}

	private Map<String, List<Pair<Integer, Integer>>> extractDatasFromXml(String xmlContent, String transactionType)
	      throws SAXException, IOException, ParserConfigurationException, XPathExpressionException {
		Map<String, List<Pair<Integer, Integer>>> topic2CountList = new HashMap<>();

		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document doc = documentBuilder.parse(new ByteArrayInputStream(xmlContent.getBytes()));

		XPath xPath = XPathFactory.newInstance().newXPath();

		String allTransactionsExpression = "/transaction/report[@domain='All']/machine[@ip='All']/type[@id='"
		      + transactionType + "']/name";

		NodeList transactionNodes = (NodeList) xPath.compile(allTransactionsExpression).evaluate(doc,
		      XPathConstants.NODESET);

		for (int i = 0; i < transactionNodes.getLength(); i++) {
			Node transactionNode = transactionNodes.item(i);

			String topic = transactionNode.getAttributes().getNamedItem("id").getNodeValue();
			topic2CountList.put(topic, new LinkedList<Pair<Integer, Integer>>());

			String allRangesExpression = "range";

			NodeList rangeNodes = (NodeList) xPath.compile(allRangesExpression).evaluate(transactionNode,
			      XPathConstants.NODESET);

			for (int j = 0; j < rangeNodes.getLength(); j++) {
				Node rangeNode = rangeNodes.item(j);
				String countStr = rangeNode.getAttributes().getNamedItem("count").getNodeValue();
				String minuteStr = rangeNode.getAttributes().getNamedItem("value").getNodeValue();
				topic2CountList.get(topic).add(
				      new Pair<Integer, Integer>(Integer.parseInt(minuteStr), Integer.parseInt(countStr)));
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
		return "ProduceAckedTriedRatioChecker";
	}
}
