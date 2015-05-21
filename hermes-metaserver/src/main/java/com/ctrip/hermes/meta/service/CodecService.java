package com.ctrip.hermes.meta.service;

import java.util.Map;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Codec;

@Named
public class CodecService {

	@Inject(ServerMetaService.ID)
	private MetaService m_metaService;

	public Codec getCodec(String topicName) {
		return m_metaService.getCodecByTopic(topicName);
	}

	public Map<String, Codec> getCodecs() {
		return m_metaService.getCodecs();
	}
}
