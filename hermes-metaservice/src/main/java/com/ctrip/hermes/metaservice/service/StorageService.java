package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.helper.Codes;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.dal.CachedDatasourceDao;
import com.ctrip.hermes.metaservice.dal.CachedStorageDao;
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;

@Named
public class StorageService {

	protected static final Logger logger = LoggerFactory.getLogger(StorageService.class);

	@Inject
	protected CachedDatasourceDao m_datasourceDao;

	@Inject
	protected CachedStorageDao m_storageDao;

	@Inject
	private CachedTopicDao topicDao;

	public void addDatasource(Datasource datasource, String dsType) throws Exception {
		com.ctrip.hermes.metaservice.model.Datasource proto = EntityToModelConverter.convert(datasource);
		proto.setId(datasource.getId());
		proto.setStorageType(dsType);
		m_datasourceDao.insert(proto);
		logger.info("Add Datasource: DS: {} done.", datasource);
	}

	public void deleteDatasource(String id, String dsType) throws Exception {
		com.ctrip.hermes.metaservice.model.Datasource proto = new com.ctrip.hermes.metaservice.model.Datasource();
		proto.setId(id);
		m_datasourceDao.deleteByPK(proto);
		logger.info("Delete Datasource: type:{}, id:{} done. updating Meta.", dsType, id);
	}

	protected com.ctrip.hermes.meta.entity.Storage fillStorage(com.ctrip.hermes.metaservice.model.Storage model, boolean fromDB)
	      throws DalException {
		com.ctrip.hermes.meta.entity.Storage entity = ModelToEntityConverter.convert(model);
		List<com.ctrip.hermes.meta.entity.Datasource> datasources = findDatasources(model.getType(), fromDB);
		for (com.ctrip.hermes.meta.entity.Datasource ds : datasources) {
			entity.addDatasource(ds);
		}
		return entity;
	}

	public Datasource findDatasource(String storageType, String datasourceId) {
		try {
			List<Datasource> datasources = findDatasources(storageType, false);
			for (Datasource d : datasources) {
				if (d.getId().equals(datasourceId)) {
					Property p = d.getProperties().get("password");
					if (p != null && p.getValue().startsWith("~{") && p.getValue().endsWith("}")) {
						p.setValue(Codes.forDecode().decode(p.getValue().substring(2, p.getValue().length() - 1)));
						d.getProperties().put("password", p);
					}
					return d;
				}
			}
		} catch (DalException e) {
			logger.warn("findDatasource failed", e);
		}
		return null;
	}

	public List<com.ctrip.hermes.meta.entity.Datasource> findDatasources(String storageType, boolean fromDB) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.Datasource> models = m_datasourceDao.list(fromDB);
		List<com.ctrip.hermes.meta.entity.Datasource> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.Datasource model : models) {
			if (storageType.equals(model.getStorageType())) {
				com.ctrip.hermes.meta.entity.Datasource entity = ModelToEntityConverter.convert(model);
				entities.add(entity);
			}
		}
		return entities;
	}

	public List<com.ctrip.hermes.meta.entity.Storage> findStorages(boolean fromDB) throws DalException {
		Collection<com.ctrip.hermes.metaservice.model.Storage> models = m_storageDao.list(fromDB);
		List<com.ctrip.hermes.meta.entity.Storage> entities = new ArrayList<>();
		for (com.ctrip.hermes.metaservice.model.Storage model : models) {
			entities.add(fillStorage(model, fromDB));
		}
		return entities;
	}

	public Map<String, Datasource> getDatasources() {
		Map<String, Datasource> idMap = new HashMap<>();
		List<Datasource> dss = new ArrayList<>();

		for (Storage storage : getStorages().values()) {
			dss.addAll(storage.getDatasources());
		}

		for (Datasource ds : dss) {
			if (idMap.containsKey(ds.getId())) {
				logger.warn("Duplicated Datasource: key {}, Datasource: {}", ds.getId(), ds.toString());
			}
			idMap.put(ds.getId(), ds);
		}
		return idMap;
	}

	public String getKafkaBrokerList() {
		List<Storage> storages = new ArrayList<>();
		try {
			storages = findStorages(false);
		} catch (DalException e) {
			logger.warn("findStorages failed", e);
		}
		for (Storage storage : storages) {
			if ("kafka".equals(storage.getType())) {
				for (Datasource ds : storage.getDatasources()) {
					for (Property property : ds.getProperties().values()) {
						if ("bootstrap.servers".equals(property.getName())) {
							return property.getValue();
						}
					}
				}
			}
		}
		return "";
	}

	public Map<String, Storage> getStorages() {
		Map<String, Storage> result = new HashMap<>();
		try {
			List<Storage> storages = findStorages(false);
			for (Storage s : storages) {
				result.put(s.getType(), s);
			}
		} catch (DalException e) {
			logger.warn("getStorages failed", e);
		}
		return result;
	}

	public String getZookeeperList() {
		List<Storage> storages = new ArrayList<>();
		try {
			storages = findStorages(false);
		} catch (DalException e) {
			logger.warn("findStorages failed", e);
		}
		for (Storage storage : storages) {
			if ("kafka".equals(storage.getType())) {
				for (Datasource ds : storage.getDatasources()) {
					for (Property property : ds.getProperties().values()) {
						if ("zookeeper.connect".equals(property.getName())) {
							return property.getValue();
						}
					}
				}
			}
		}
		return "";
	}

	public void updateDatasource(Datasource dsEntity) throws Exception {
		com.ctrip.hermes.metaservice.model.Datasource dsModel = m_datasourceDao.findByPK(dsEntity.getId());
		dsModel.setProperties(JSON.toJSONString(dsEntity.getProperties()));
		m_datasourceDao.updateByPK(dsModel);
	}
}
