package com.ctrip.hermes.metaservice.service;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.metaservice.dal.CachedEndpointDao;

@Named
public class EndpointService {

	protected static final Logger logger = LoggerFactory.getLogger(EndpointService.class);
	
	@Inject
	protected CachedEndpointDao m_endpointDao;
	
	@Inject
	private DefaultPortalMetaService metaService;
	
	public Map<String, Endpoint> getEndpoints() {
		Map<String, Endpoint> result = new HashMap<String, Endpoint>();
		try {
			for (Endpoint e : metaService.findEndpoints()) {
				result.put(e.getId(), e);
			}
		} catch (DalException e) {
			logger.warn("getEndpoints failed", e);
		}
		return result;
	}

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
}
