package com.ctrip.hermes.collector.collector;

import java.io.InputStream;
import java.io.StringWriter;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;

import com.ctrip.hermes.collector.datasource.HttpDatasource;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.record.TimeMomentRecord;
import com.ctrip.hermes.collector.record.TimeWindowRecord;
import com.ctrip.hermes.collector.utils.TypeConverter;

/**
 * @author tenglinxiao
 *
 */

@Component
public class CatHttpCollector extends HttpCollector {
	private static final Logger LOGGER = LoggerFactory.getLogger(EsHttpCollector.class);

	@SuppressWarnings("unchecked")
	@Override
	public Record<Document> handleResponse(CollectorContext context, InputStream input) {
		CatHttpCollectorContext ctx = (CatHttpCollectorContext)context;
		try {
			Record<Document> record = context.getType().newRecord();
			if (record instanceof TimeWindowRecord) {
				TimeWindowRecord<Document> timeWindowRecord = (TimeWindowRecord<Document>)record;
				timeWindowRecord.setStartDate(TypeConverter.toDate(ctx.getFrom()));		
				timeWindowRecord.setEndDate(TypeConverter.toDate(ctx.getTo()));
			} else {
				TimeMomentRecord<Document> timeMomentRecord = (TimeMomentRecord<Document>)record;
				timeMomentRecord.setMoment(TypeConverter.toDate(ctx.getFrom()));
			}
			
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
			record.setData(documentBuilder.parse(input));
//			StringWriter writer = new StringWriter();
//			TransformerFactory.newInstance().newTransformer().transform(new DOMSource(record.getData()), new StreamResult(writer));
//			LOGGER.info("Xml response: {}", writer.toString());
			context.setSucceed(true);
			return record;
		} catch (Exception e) {
			LOGGER.error("Failed to parse response for cat http request!", e);
			return null;
		} finally {
			LOGGER.info("Issued request on cat with context {}", ctx);
		}
	}
	
	public static class CatHttpCollectorContext extends HttpCollectorContext {
		private long m_from;
		private long m_to;

		public CatHttpCollectorContext(HttpDatasource datasource, RecordType type) {
			super(datasource, type);
		}
		
		public long getFrom() {
			return m_from;
		}

		public void setFrom(long from) {
			m_from = from;
		}

		public long getTo() {
			return m_to;
		}

		public void setTo(long to) {
			m_to = to;
		}

	}
	
}
