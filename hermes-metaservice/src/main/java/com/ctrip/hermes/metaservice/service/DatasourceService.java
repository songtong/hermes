package com.ctrip.hermes.metaservice.service;

import java.util.ArrayList;
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
import com.ctrip.hermes.metaservice.dal.CachedTopicDao;

@Named
public class DatasourceService {

	protected static final Logger logger = LoggerFactory.getLogger(DatasourceService.class);

	@Inject
	protected CachedDatasourceDao m_datasourceDao;

	@Inject
	private CachedTopicDao topicDao;

	@Inject
	private DefaultPortalMetaService metaService;

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

	public void updateDatasource(Datasource dsEntity) throws Exception {
		com.ctrip.hermes.metaservice.model.Datasource dsModel = m_datasourceDao.findByPK(dsEntity.getId());
		dsModel.setProperties(JSON.toJSONString(dsEntity.getProperties()));
		m_datasourceDao.updateByPK(dsModel);
	}

	public Storage findStorageByTopic(String topicName) {
		try {
			com.ctrip.hermes.metaservice.model.Topic topic = topicDao.findByName(topicName);
			List<Storage> storages = metaService.findStorages();
			for (Storage s : storages) {
				if (s.getType().equals(topic.getStorageType()))
					return s;
			}
		} catch (Exception e) {
			logger.warn("findStorageByTopic failed", e);
		}
		return null;
	}

	public Datasource findDatasource(String storageType, String datasourceId) {
		try {
			List<Datasource> datasources = metaService.findDatasources(storageType);
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

	public Map<String, Storage> getStorages() {
		Map<String, Storage> result = new HashMap<>();
		try {
			List<Storage> storages = metaService.findStorages();
			for (Storage s : storages) {
				result.put(s.getType(), s);
			}
		} catch (DalException e) {
			logger.warn("getStorages failed", e);
		}
		return result;
	}

}
