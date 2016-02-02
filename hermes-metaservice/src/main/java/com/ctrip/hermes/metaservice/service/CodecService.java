package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.metaservice.converter.ModelToEntityConverter;
import com.ctrip.hermes.metaservice.dal.CachedCodecDao;

@Named
public class CodecService {

	@Inject
	private CachedCodecDao codecDao;

	public Map<String, Codec> getCodecs() {
		Map<String, Codec> result = new HashMap<String, Codec>();
		try {
			for (Codec codec : findCodecs(false)) {
				result.put(codec.getType(), codec);
			}
		} catch (DalException e) {
			e.printStackTrace();
		}
		return result;
	}

	public List<com.ctrip.hermes.meta.entity.Codec> findCodecs(boolean fromDB) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.Codec> models = codecDao.list(fromDB);
		List<com.ctrip.hermes.meta.entity.Codec> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.Codec model : models) {
			com.ctrip.hermes.meta.entity.Codec entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}
}
