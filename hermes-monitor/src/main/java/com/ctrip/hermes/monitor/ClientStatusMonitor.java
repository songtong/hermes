package com.ctrip.hermes.monitor;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Component
public class ClientStatusMonitor {

	private static final Logger log = LoggerFactory.getLogger(ClientStatusMonitor.class);

	private static final String CAT_DATE_PATTERN = "yyyyMMddkk";

	private static final String CAT_CROSS_TRANSACTION_URL_PATTERN = "http://cat.ctripcorp.com/cat/r/t?op=graphs&domain=All&date=%s&ip=All&type=%s&forceDownload=xml";

	private static final int CAT_CONNECT_TIMEOUT = 5 * 1000;

	private static final int CAT_READ_TIMEOUT = 20 * 1000;

	@Scheduled(cron = "0 */5 * * * *")
	public void execute() {
		
		log.info("Client status monitor started...");

		Date currentDate = new Date();
		
//		checkProduceLatency(currentDate);

//		String xml = getCatTransactionData(currentDate, "Message.Produce.Elapse");

		log.info("Client status monitor completed...");
	}
	
	

	private String getCatTransactionData(Date date, String transactionType) throws Exception {

		String url = String.format(CAT_CROSS_TRANSACTION_URL_PATTERN,
		      new SimpleDateFormat(CAT_DATE_PATTERN).format(date), transactionType);

		return Request.Get(url)//
		      .connectTimeout(CAT_CONNECT_TIMEOUT)//
		      .socketTimeout(CAT_READ_TIMEOUT)//
		      .execute()//
		      .returnContent()//
		      .asString()//
		;
	}

}
