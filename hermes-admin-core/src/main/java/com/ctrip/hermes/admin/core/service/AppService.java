package com.ctrip.hermes.admin.core.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.converter.ModelToEntityConverter;
import com.ctrip.hermes.admin.core.dal.CachedAppDao;

@Named
public class AppService {

	@Inject
	private CachedAppDao appDao;

	public List<com.ctrip.hermes.meta.entity.App> findApps(boolean fromDB) throws DalException {
		Collection<com.ctrip.hermes.admin.core.model.App> models = appDao.list(fromDB);
		List<com.ctrip.hermes.meta.entity.App> entities = new ArrayList<>();
		for (com.ctrip.hermes.admin.core.model.App model : models) {
			com.ctrip.hermes.meta.entity.App entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}
}
