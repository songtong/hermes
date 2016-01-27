package com.ctrip.hermes.core.message.payload;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class PayloadCodecFactory {

	private static ConcurrentMap<String, PayloadCodecCompositor> compositorCache = new ConcurrentHashMap<>();

	public static PayloadCodec getCodecByTopicName(String topicName) {
		MetaService metaService = PlexusComponentLocator.lookup(MetaService.class);

		Topic topic = metaService.findTopicByName(topicName);
		return getCodecByType(topic.getCodecType());
	}

	public static PayloadCodec getCodecByType(String codecString) {
		String[] codecTypes = splitCodecType(codecString);

		if (codecTypes.length == 0) {
			throw new IllegalArgumentException(String.format("codec type '%s' is illegal", codecString));
		} else if (codecTypes.length == 1) {
			return PlexusComponentLocator.lookup(PayloadCodec.class, codecString);
		} else {
			PayloadCodecCompositor cached = compositorCache.get(codecString);
			
			if (cached != null) {
				return cached;
			} else {
				PayloadCodecCompositor compositor = new PayloadCodecCompositor(codecString);
				for (String codecType : codecTypes) {
					PayloadCodec codec = PlexusComponentLocator.lookup(PayloadCodec.class, codecType);
					compositor.addPayloadCodec(codec);
				}

				compositorCache.putIfAbsent(codecString, compositor);
				return compositorCache.get(codecString);
			}
		}

	}

	private static String[] splitCodecType(String codecType) {
		String[] parts = codecType.split(",");
		for (int i = 0; i < parts.length; i++) {
			parts[i] = parts[i].trim();
		}
		return parts;
	}

}
