package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.dal.CachedAppDao;

@Named
public class AppService {

	@Inject
	private CachedAppDao appDao;

	public List<com.ctrip.hermes.meta.entity.App> findApps() throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.App> models = appDao.list();
		List<com.ctrip.hermes.meta.entity.App> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.App model : models) {
			com.ctrip.hermes.meta.entity.App entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}
}
