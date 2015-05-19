package com.ctrip.hermes.core.message.payload;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class PayloadCodecFactory {

	public static PayloadCodec getCodecByTopicName(String topic) {
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);

		com.ctrip.hermes.meta.entity.Codec codecEntity = metaService.getCodecByTopic(topic);
		return getCodecByType(codecEntity.getType());
	}

	public static PayloadCodec getCodecByType(String codecType) {
		return PlexusComponentLocator.lookup(PayloadCodec.class, codecType);
	}

}
