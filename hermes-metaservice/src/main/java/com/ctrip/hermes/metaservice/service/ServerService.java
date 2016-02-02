package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.converter.ModelToEntityConverter;
import com.ctrip.hermes.metaservice.dal.CachedServerDao;

@Named
public class ServerService {

	@Inject
	protected CachedServerDao m_serverDao;

	public List<com.ctrip.hermes.meta.entity.Server> findServers(boolean fromDB) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.Server> models = m_serverDao.list(fromDB);
		List<com.ctrip.hermes.meta.entity.Server> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.Server model : models) {
			com.ctrip.hermes.meta.entity.Server entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}
}
