package com.ctrip.hermes.monitor.checker;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

import org.apache.http.client.fluent.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.monitor.config.MonitorConfig;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public abstract class CatBasedChecker implements Checker {
	protected static final SimpleDateFormat DEFAULT_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static final String CAT_DATE_PATTERN = "yyyyMMddkk";

	@Autowired
	protected MonitorConfig m_config;

	public static class Timespan {
		private Date m_startHour;

		private List<Integer> m_minutes = new LinkedList<>();

		public Date getStartHour() {
			return m_startHour;
		}

		public void setStartHour(Date startHour) {
			m_startHour = startHour;
		}

		public List<Integer> getMinutes() {
			return m_minutes;
		}

		public void addMinute(int minute) {
			m_minutes.add(minute);
		}

	}

	protected static class CatRangeEntity {
		private int m_minInHour;

		private int m_count;

		private int m_fails;

		private float m_sum;

		private float m_avg;

		public CatRangeEntity(int minInHour, int count, int fails, float sum, float avg) {
			m_minInHour = minInHour;
			m_count = count;
			m_fails = fails;
			m_sum = sum;
			m_avg = avg;
		}

		public int getMinInHour() {
			return m_minInHour;
		}

		public int getCount() {
			return m_count;
		}

		public int getFails() {
			return m_fails;
		}

		public float getSum() {
			return m_sum;
		}

		public float getAvg() {
			return m_avg;
		}
	}

	protected Timespan calTimespan(Date toDate, int minutesBefore) {
		if (minutesBefore > 60 || minutesBefore <= 0) {
			throw new IllegalArgumentException(String.format("MinutesBefore invalid(toDate=%s, minutesBefore=%s).",
			      toDate, minutesBefore));
		}

		Timespan timespan = new Timespan();

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(toDate);

		int minute = calendar.get(Calendar.MINUTE);

		if (minute == 0) {
			calendar.add(Calendar.HOUR_OF_DAY, -1);
			timespan.setStartHour(calendar.getTime());

			for (int i = 0; i < minutesBefore; i++) {
				timespan.addMinute(59 - i);
			}
		} else {
			if (minute >= minutesBefore) {
				timespan.setStartHour(calendar.getTime());

				for (int i = 1; i <= minutesBefore; i++) {
					timespan.addMinute(minute - i);
				}
			} else {
				throw new IllegalArgumentException(String.format(
				      "There is not enough data for query(toDate=%s, minuteBefore=%s).", toDate, minutesBefore));
			}
		}

		return timespan;
	}

	protected String formatToCatUrlTime(Date date) {
		return new SimpleDateFormat(CAT_DATE_PATTERN).format(date);
	}

	protected String curl(String url, int connectTimeoutMillis, int readTimeoutMillis) throws IOException {
		try {
			return Request.Get(url)//
			      .connectTimeout(connectTimeoutMillis)//
			      .socketTimeout(readTimeoutMillis)//
			      .execute()//
			      .returnContent()//
			      .asString();
		} catch (IOException e) {
			throw new IOException(String.format("Failed to fetch data from url %s", url), e);
		}
	}

	@SuppressWarnings("unchecked")
	protected List<String> getMetaServerList(String url) throws IOException {
		List<String> list = new ArrayList<>();
		String listStr = curl(url, 30000, 30000);
		if (!StringUtils.isBlank(listStr)) {
			list = new ArrayList<>(CollectionUtil.collect(JSON.parseObject(listStr, new TypeReference<List<String>>() {
			}), new Transformer() {
				@Override
				public Object transform(Object input) {
					return input != null ? ((String) input).split(":")[0] : input;
				}
			}));
		}
		return list;
	}

	protected Map<String, Map<Integer, CatRangeEntity>> getCatCrossDomainData(Timespan timespan, String catType)
	      throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
		String transactionReportXml = getCatCrossDomainTransactionReport(timespan, catType);
		return extractCatNameRangesFromXml(transactionReportXml, catType);
	}

	protected String getCatCrossDomainTransactionReport(Timespan timespan, String transactionType) throws IOException {
		String catReportUrl = m_config.getCatBaseUrl()
		      + String.format(m_config.getCatCrossTransactionUrlPattern(), formatToCatUrlTime(timespan.getStartHour()),
		            transactionType);
		String transactionReportXml = curl(catReportUrl, m_config.getCatConnectTimeout(), m_config.getCatReadTimeout());
		return transactionReportXml;
	}

	protected Map<String, Map<Integer, CatRangeEntity>> extractCatNameRangesFromXml(String xmlContent,
	      String transactionType) throws SAXException, IOException, ParserConfigurationException,
	      XPathExpressionException {
		Map<String, Map<Integer, CatRangeEntity>> catName2CountList = new HashMap<>();

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

			String catName = transactionNode.getAttributes().getNamedItem("id").getNodeValue();
			if (!"All".equals(catName)) {
				catName2CountList.put(catName, new LinkedHashMap<Integer, CatRangeEntity>());

				String allRangesExpression = "range";

				NodeList rangeNodes = (NodeList) xPath.compile(allRangesExpression).evaluate(transactionNode,
				      XPathConstants.NODESET);

				for (int j = 0; j < rangeNodes.getLength(); j++) {
					Node rangeNode = rangeNodes.item(j);
					int minInHour = Integer.parseInt(rangeNode.getAttributes().getNamedItem("value").getNodeValue());
					int count = Integer.parseInt(rangeNode.getAttributes().getNamedItem("count").getNodeValue());
					int fails = Integer.parseInt(rangeNode.getAttributes().getNamedItem("fails").getNodeValue());
					float sum = Float.parseFloat(rangeNode.getAttributes().getNamedItem("sum").getNodeValue());
					float avg = Float.parseFloat(rangeNode.getAttributes().getNamedItem("avg").getNodeValue());
					catName2CountList.get(catName).put(minInHour, new CatRangeEntity(minInHour, count, fails, sum, avg));
				}
			}
		}

		return catName2CountList;
	}

	@Override
	public CheckerResult check(Date toDate, int minutesBefore) {
		CheckerResult result = new CheckerResult();

		try {
			Timespan timespan = calTimespan(toDate, minutesBefore);

			if (m_config.isMonitorCheckerEnable()) {
				doCheck(timespan, result);
			}
		} catch (Exception e) {
			result.setRunSuccess(false);
			result.setErrorMessage(e.getMessage());
			result.setException(e);
		}

		return result;
	}

	protected abstract void doCheck(Timespan timespan, CheckerResult result) throws Exception;

}