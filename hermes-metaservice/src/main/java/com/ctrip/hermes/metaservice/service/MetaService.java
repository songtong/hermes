package com.ctrip.hermes.metaservice.service;

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
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface MetaService {

	Meta getMetaEntity();

	Meta refreshMeta() throws DalException;

	Meta buildNewMeta() throws DalException;

	Meta previewNewMeta() throws DalException;

	List<App> findApps() throws DalException;

	List<Codec> findCodecs() throws DalException;

	List<ConsumerGroup> findConsumerGroups(com.ctrip.hermes.metaservice.model.Topic topicModel) throws DalException;

	List<Datasource> findDatasources(com.ctrip.hermes.metaservice.model.Storage storageModel) throws DalException;

	List<Endpoint> findEndpoints() throws DalException;

	List<Partition> findPartitions(com.ctrip.hermes.metaservice.model.Topic topicModel) throws DalException;

	List<Producer> findProducers(com.ctrip.hermes.metaservice.model.Topic topicModel) throws DalException;

	List<Server> findServers() throws DalException;

	List<Storage> findStorages() throws DalException;

	Storage getStorage(String type) throws DalException;

	List<Topic> findTopics() throws DalException;

}
