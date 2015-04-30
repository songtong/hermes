package com.ctrip.hermes.core.codec;

import java.util.HashMap;
import java.util.Map;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Property;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class PayloadCodecFactory {

	public static PayloadCodec getCodecByTopicName(String topic) {
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);

		com.ctrip.hermes.meta.entity.Codec codecEntity = metaService.getCodecByTopic(topic);
		return getCodec(codecEntity);
	}

	private static PayloadCodec getCodec(com.ctrip.hermes.meta.entity.Codec codecEntity) {
	   PayloadCodec codec = PlexusComponentLocator.lookup(PayloadCodec.class, codecEntity.getType());
		Map<String, String> configs = new HashMap<>();
		for (Property property : codecEntity.getProperties()) {
			configs.put(property.getName(), property.getValue());
		}
		codec.configure(configs);
		return codec;
   }

	public static PayloadCodec getCodecByType(String codecType) {
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);
		com.ctrip.hermes.meta.entity.Codec codecEntity = metaService.getCodecByType(codecType);
		return getCodec(codecEntity);
   }

}
