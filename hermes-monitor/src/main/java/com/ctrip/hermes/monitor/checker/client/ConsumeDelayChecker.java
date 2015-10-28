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

import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.ctrip.hermes.metaservice.monitor.event.ConsumeDelayTooLargeEvent;
import com.ctrip.hermes.monitor.checker.CheckerResult;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Component(value = "ConsumeDelayChecker")
public class ConsumeDelayChecker extends CatTransactionCrossReportBasedChecker {

	private static final String CAT_TRANSACTION_TYPE = "Message.Consume.Latency";

	// key: topic, value: threshold in millisecond
	private static final Map<String, Double> TOPICS_2_DELAY_THRESHOLD = new HashMap<>();

	static {
		TOPICS_2_DELAY_THRESHOLD.put("leo_test_11111", 1 * 1000d);// for test
	}

	@Override
	protected void doCheck(String transactionReportXml, Timespan timespan, CheckerResult result) throws Exception {
		Map<String, List<Pair<Integer, Double>>> topic2DelayList = extractDelayDatasFromXml(transactionReportXml);
		bizCheck(topic2DelayList, timespan, result);
	}

	private void bizCheck(Map<String, List<Pair<Integer, Double>>> topic2DelayList, Timespan timespan,
	      CheckerResult result) {

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		for (Map.Entry<String, List<Pair<Integer, Double>>> entry : topic2DelayList.entrySet()) {
			String topic = entry.getKey();
			List<Pair<Integer, Double>> delayList = entry.getValue();

			if (TOPICS_2_DELAY_THRESHOLD.containsKey(topic)) {
				for (Pair<Integer, Double> pair : delayList) {
					int minute = pair.getKey();
					double delay = pair.getValue();

					Double threshold = TOPICS_2_DELAY_THRESHOLD.get(topic);

					if (timespan.getMinutes().contains(minute) && delay > threshold) {
						ConsumeDelayTooLargeEvent monitorEvent = new ConsumeDelayTooLargeEvent();
						monitorEvent.setTopic(topic);
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

		result.setRunSuccess(true);
	}

	private Map<String, List<Pair<Integer, Double>>> extractDelayDatasFromXml(String xmlContent) throws SAXException,
	      IOException, ParserConfigurationException, XPathExpressionException {
		Map<String, List<Pair<Integer, Double>>> topic2DelayList = new HashMap<>();

		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document doc = documentBuilder.parse(new ByteArrayInputStream(xmlContent.getBytes()));

		XPath xPath = XPathFactory.newInstance().newXPath();

		String allTransactionsExpression = "/transaction/report/machine[@ip='All']/type[@id='" + CAT_TRANSACTION_TYPE
		      + "']/name";

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

	@Override
	protected String getTransactionType() {
		return CAT_TRANSACTION_TYPE;
	}

}
