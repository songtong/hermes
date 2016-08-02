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
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.converter.EntityToModelConverter;
import com.ctrip.hermes.metaservice.converter.ModelToEntityConverter;
import com.ctrip.hermes.metaservice.dal.CachedEndpointDao;
import com.ctrip.hermes.metaservice.model.EndpointEntity;

@Named
public class EndpointService {

	protected static final Logger logger = LoggerFactory.getLogger(EndpointService.class);

	@Inject
	protected CachedEndpointDao m_endpointDao;

	@Inject
	protected TopicService m_topicService;

	public synchronized void addEndpoint(Endpoint endpoint) throws Exception {
		com.ctrip.hermes.metaservice.model.Endpoint proto = EntityToModelConverter.convert(endpoint);
		m_endpointDao.insert(proto);
		logger.info("Add Endpoint: {} done.", endpoint);
	}

	public void deleteEndpoint(String endpointId) throws IllegalStateException, Exception {
		Endpoint e = findEndpoint(endpointId);
		if (isUnique(e)) {
			if (hasTopicOnGroup(e.getGroup())) {
				throw new IllegalStateException(String.format(
				      "Topic exits on group '%s'! Please migrate topic(s) or add one another endpoint to this group first!",
				      e.getGroup()));
			}
		}
		com.ctrip.hermes.metaservice.model.Endpoint proto = new com.ctrip.hermes.metaservice.model.Endpoint();
		proto.setId(endpointId);
		m_endpointDao.deleteByPK(proto);
		logger.info("Delete Endpoint: id:{} done.", endpointId);
	}

	private boolean hasTopicOnGroup(String group) throws DalException {
		for (Topic topic : m_topicService.findTopicEntities(false)) {
			if (topic.getBrokerGroup().equals(group)) {
				return true;
			}
		}
		return false;
	}

	private boolean isUnique(Endpoint e) throws DalException {
		for (Endpoint endpoint : findEndpoints(false)) {
			if (endpoint.getGroup() == e.getGroup() && endpoint.getType() == e.getType() && endpoint.getId() != e.getId()) {
				return false;
			}
		}
		return true;
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

	public com.ctrip.hermes.meta.entity.Endpoint findEndpoint(String endpointId) throws DalException {
		com.ctrip.hermes.metaservice.model.Endpoint model = m_endpointDao.findByPK(endpointId);
		return model != null ? ModelToEntityConverter.convert(model) : null;
	}

	public void updateEndpoint(Endpoint endpoint) throws IllegalStateException, Exception {
		Endpoint e = findEndpoint(endpoint.getId());
		if (isUnique(e)) {
			if (hasTopicOnGroup(e.getGroup())) {
				throw new IllegalStateException(String.format(
				      "Topic exits on group %s! Please migrate topic(s) or add one another endpoint to this group first!",
				      e.getGroup()));
			}
		}
		com.ctrip.hermes.metaservice.model.Endpoint proto = EntityToModelConverter.convert(endpoint);
		m_endpointDao.updateByPK(proto, EndpointEntity.UPDATESET_FULL);
		logger.info("Add Endpoint: {} done.", endpoint);
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
