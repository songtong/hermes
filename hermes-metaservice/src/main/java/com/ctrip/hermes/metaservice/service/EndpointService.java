package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.metaservice.converter.EntityToModelConverter;
import com.ctrip.hermes.metaservice.converter.ModelToEntityConverter;
import com.ctrip.hermes.metaservice.dal.CachedEndpointDao;

@Named
public class EndpointService {

	protected static final Logger logger = LoggerFactory.getLogger(EndpointService.class);

	@Inject
	protected CachedEndpointDao m_endpointDao;

	public synchronized void addEndpoint(Endpoint endpoint) throws Exception {
		com.ctrip.hermes.metaservice.model.Endpoint proto = EntityToModelConverter.convert(endpoint);
		m_endpointDao.insert(proto);
		logger.info("Add Endpoint: {} done.", endpoint);
	}

	public void deleteEndpoint(String endpointId) throws Exception {
		com.ctrip.hermes.metaservice.model.Endpoint proto = new com.ctrip.hermes.metaservice.model.Endpoint();
		proto.setId(endpointId);
		m_endpointDao.deleteByPK(proto);
		logger.info("Delete Endpoint: id:{} done.", endpointId);
	}

	public List<com.ctrip.hermes.meta.entity.Endpoint> findEndpoints(boolean fromDB) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.Endpoint> models = m_endpointDao.list(fromDB);
		List<com.ctrip.hermes.meta.entity.Endpoint> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.Endpoint model : models) {
			com.ctrip.hermes.meta.entity.Endpoint entity = ModelToEntityConverter.convert(model);
			entities.add(entity);
		}
		return entities;
	}

	public Map<String, Endpoint> getEndpoints() {
		Map<String, Endpoint> result = new HashMap<String, Endpoint>();
		try {
			for (Endpoint e : findEndpoints(false)) {
				result.put(e.getId(), e);
			}
		} catch (DalException e) {
			logger.warn("getEndpoints failed", e);
		}
		return result;
	}
}
