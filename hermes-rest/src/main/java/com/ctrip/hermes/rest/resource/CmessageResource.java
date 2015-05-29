package com.ctrip.hermes.rest.resource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.rest.common.MetricsConstant;
import com.ctrip.hermes.rest.common.RestConstant;
import com.ctrip.hermes.rest.service.CmessageTransferService;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;

@Path("/cmessage")
public class CmessageResource {

	private static Logger log = LoggerFactory.getLogger(CmessageResource.class);

	private static AtomicInteger integer = new AtomicInteger(0);

	CmessageTransferService service = CmessageTransferService.getInstance();

	/**
	 * if you do post request via POSTMAN, remember to add Headers (Content-Type:application/json)
	 */
	@POST
	@Consumes("application/json")
	@Produces(MediaType.APPLICATION_JSON)
	public Integer getCollectorInfo(Map<String, String> map) {

		Transaction t = Cat.newTransaction(RestConstant.CAT_TYPE, RestConstant.CAT_NAME);

		metricsAddCount(MetricsConstant.CmessageReceive);

		String topic = map.get("Topic");
		// in fact, content in map is byte[]...
		String content = map.get("Content");
		String header = map.get("Header");
		try {
			if (null == topic || null == content || null == header) {
				StringBuilder sb = new StringBuilder();
				sb.append("Invalid Message: ");
				sb.append(map);

				log.error(sb.toString());
				Cat.logEvent(RestConstant.CAT_TYPE, RestConstant.CAT_NAME, Message.SUCCESS, sb.toString());
				throw new RuntimeException("invalid message");
			}

			// call service
			service.transfer(topic, content, header);

			t.setStatus(Message.SUCCESS);
			metricsAddCount(MetricsConstant.CmessageReceive);
		} catch (Exception e) {
			log.error("", e);
			t.setStatus(e);
		}
		t.complete();

		Cat.logEvent(RestConstant.CAT_TYPE, RestConstant.CAT_NAME, Message.SUCCESS, map.toString());

		map.remove("Content");
		log.info("Content:" + content + ",Properties:" + map + "");

		return integer.addAndGet(1);
	}

	private void metricsAddCount(String cmessageReceive) {
		Cat.logMetricForCount(cmessageReceive);
	}
}
