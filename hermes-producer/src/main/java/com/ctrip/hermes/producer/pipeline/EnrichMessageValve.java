package com.ctrip.hermes.producer.pipeline;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.message.MessagePropertyNames;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.spi.Valve;
import com.ctrip.hermes.core.utils.StringUtils;

@Named(type = Valve.class, value = EnrichMessageValve.ID)
public class EnrichMessageValve implements Valve {
	private static final Logger log = LoggerFactory.getLogger(EnrichMessageValve.class);

	public static final String ID = "enrich";

	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {
		ProducerMessage<?> msg = (ProducerMessage<?>) payload;
		String ip = Networks.forIp().getLocalHostAddress();
		enrichPartitionKey(msg, ip);

		if (msg.isWithHeader()) {
			enrichRefKey(msg);
			enrichMessageProperties(msg, ip);
		}
		ctx.next(payload);
	}

	private void enrichRefKey(ProducerMessage<?> msg) {
		if (StringUtils.isEmpty(msg.getKey())) {
			String refKey = UUID.randomUUID().toString();
			log.info("Ref key not set, will set uuid as ref key(topic={}, ref key={})", msg.getTopic(), refKey);
			msg.setKey(refKey);
		}
	}

	private void enrichMessageProperties(ProducerMessage<?> msg, String ip) {
		msg.addDurableSysProperty(MessagePropertyNames.PRODUCER_IP, ip);
	}

	private void enrichPartitionKey(ProducerMessage<?> msg, String ip) {
		if (StringUtils.isEmpty(msg.getPartitionKey())) {
			log.info("Parition key not set, will set ip as partition key(topic={}, ip={})", msg.getTopic(), ip);
			msg.setPartitionKey(ip);
		}
	}

}
