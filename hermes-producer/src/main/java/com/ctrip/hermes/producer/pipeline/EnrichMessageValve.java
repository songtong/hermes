package com.ctrip.hermes.producer.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;
import org.unidal.net.Networks;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.pipeline.PipelineContext;
import com.ctrip.hermes.core.pipeline.spi.Valve;
import com.ctrip.hermes.core.utils.StringUtils;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.spi.MessageTree;

@Named(type = Valve.class, value = EnrichMessageValve.ID)
public class EnrichMessageValve implements Valve {
	private static final Logger log = LoggerFactory.getLogger(EnrichMessageValve.class);

	public static final String ID = "enrich";

	@Override
	public void handle(PipelineContext<?> ctx, Object payload) {
		ProducerMessage<?> msg = (ProducerMessage<?>) payload;
		String topic = msg.getTopic();
		String partitionKey = msg.getPartitionKey();

		if (StringUtils.isEmpty(topic)) {
			log.error("Topic not set, won't send");
			return;
		}

		if (StringUtils.isEmpty(partitionKey)) {
			System.out.println("Parition key not set, will set ip as default partition key automatically(topic={})");
			log.warn("Parition key not set, will set ip as default partition key automatically(topic={})", msg.getTopic());
			partitionKey = Networks.forIp().getLocalHostAddress();
			MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
			try {
				String rootMsgId = tree.getRootMessageId();
				String msgId = Cat.getCurrentMessageId();
				rootMsgId = rootMsgId == null ? msgId : rootMsgId;
				Cat.logEvent("Message:" + topic, "Enrich with partitionKey:" + partitionKey, Event.SUCCESS, "partition="
				      + partitionKey);
			} catch (RuntimeException | Error e) {
				Cat.logError(e);
				throw e;
			}
		}
		ctx.next(payload);
	}

}
