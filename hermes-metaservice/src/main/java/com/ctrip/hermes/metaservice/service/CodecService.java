package com.ctrip.hermes.metaservice.service;

import java.util.HashMap;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Codec;

@Named
public class CodecService {

	@Inject
	private DefaultPortalMetaService m_metaService;

	public Map<String, Codec> getCodecs() {
		Map<String, Codec> result = new HashMap<String, Codec>();
		try {
			for (Codec codec : m_metaService.findCodecs()) {
				result.put(codec.getType(), codec);
			}
		} catch (DalException e) {
			e.printStackTrace();
		}
		return result;
	}

	public Codec findCodecByType(String type) {
		return getCodecs().get(type);
	}

}
