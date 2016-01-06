package com.ctrip.hermes.metaserver.fulltest;

import java.util.List;

import org.unidal.dal.jdbc.DalException;

import com.ctrip.hermes.meta.entity.App;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Producer;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.metaservice.model.Storage;
import com.ctrip.hermes.metaservice.model.Topic;
import com.ctrip.hermes.metaservice.service.MetaService;

public class MockMetaService implements MetaService {

	@Override
	public Meta refreshMeta() {
		Meta meta = null;
		try {
			meta = loadMeta();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return meta;
	}

	protected Meta loadMeta() throws Exception {

		String fileName = MetaServerBaseTest.metaXmlFile;

		return MetaServerBaseTest.MetaHelper.loadMeta(fileName);
	}

	@Override
	public Meta getMetaEntity() {
		return null;
	}

	@Override
	public List<App> findApps() throws DalException {
		return null;
	}

	@Override
	public List<Codec> findCodecs() throws DalException {
		return null;
	}

	@Override
	public List<ConsumerGroup> findConsumerGroups(Topic topicModel) throws DalException {
		return null;
	}

	@Override
	public List<Datasource> findDatasources(Storage storageModel) throws DalException {
		return null;
	}

	@Override
	public List<Endpoint> findEndpoints() throws DalException {
		return null;
	}

	@Override
	public List<Partition> findPartitions(Topic topicModel) throws DalException {
		return null;
	}

	@Override
	public List<Producer> findProducers(Topic topicModel) throws DalException {
		return null;
	}

	@Override
	public List<Server> findServers() throws DalException {
		return null;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Storage> findStorages() throws DalException {
		return null;
	}

	@Override
	public List<com.ctrip.hermes.meta.entity.Topic> findTopics() throws DalException {
		return null;
	}

	@Override
	public com.ctrip.hermes.meta.entity.Storage getStorage(String type) throws DalException {
		return null;
	}

}
