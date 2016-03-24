package com.ctrip.hermes.portal.service.tag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.dal.CachedDatasourceDao;
import com.ctrip.hermes.portal.dal.datasourcetag.DatasourceTag;
import com.ctrip.hermes.portal.dal.datasourcetag.DatasourceTagDao;
import com.ctrip.hermes.portal.dal.datasourcetag.DatasourceTagEntity;
import com.ctrip.hermes.portal.dal.tag.CachedTagDao;
import com.ctrip.hermes.portal.dal.tag.Tag;

@Named(type=DefaultTagService.class)
public class DefaultTagService {
	private static final Logger log = LoggerFactory.getLogger(DefaultTagService.class);
	
	@Inject
	private CachedTagDao m_tagDao;
	
	@Inject 
	private CachedDatasourceDao m_datasourceDao;
	
	@Inject
	private DatasourceTagDao m_datasourceTagDao;
	
	@Inject
	private TransactionManager m_transactionManager;
	
	// List all tags grouped by datasource.
	public Map<String, List<Tag>> listDatasourcesTags() {
		try {
			Map<String, List<Tag>> datasourcesTags = new HashMap<String, List<Tag>>();
			List<DatasourceTag> mapping = m_datasourceTagDao.findAll(DatasourceTagEntity.READSET_FULL);
			for (DatasourceTag dt : mapping) {
				if (datasourcesTags.get(dt.getDatasourceId()) != null) {
					datasourcesTags.put(dt.getDatasourceId(), new ArrayList<Tag>());
				}
				datasourcesTags.get(dt.getDatasourceId()).add(m_tagDao.findByPK(dt.getTagId()));
			}
			return datasourcesTags;
		} catch (DalException e) {
			log.error("Failed to list all datasources tags from db!", e);
			return null;
		}
	}
	
	// List all tags attached to one datasource.
	public List<Tag> listDatasourceTags(String datasourceId) throws DalException {
		try {
			if (validateDatasource(datasourceId)) {
				List<Tag> tags = new ArrayList<Tag>();
				List<DatasourceTag> mapping = m_datasourceTagDao.findByDatasource(datasourceId, DatasourceTagEntity.READSET_FULL);
				for (DatasourceTag dt : mapping) {
					tags.add(m_tagDao.findByPK(dt.getTagId()));
				}
				return tags;
			}
		} catch (DalException e) {
			log.error("Failed to list all datasource tags for datasource: {}", datasourceId, e);
			return null;
		}
		
		throw new DalException(String.format("Can NOT find datasouce: %s", datasourceId));
	}

	// List all tags grouped by tag-group name.
	public Map<String, List<Tag>> listTags() {
		try {
			Map<String, List<Tag>> groupTags = new HashMap<String, List<Tag>>();
			Collection<Tag> tags = m_tagDao.list(false);
			for (Tag t : tags) {
				if (groupTags.get(t.getGroup()) != null) {
					groupTags.put(t.getGroup(), new ArrayList<Tag>());
				}
				groupTags.get(t.getGroup()).add(t);
			}
			return groupTags;
		} catch (DalException e) {
			log.error("Failed to list all tags from cache! ", e);
			return null;
		}
	}
	
	// Add datasource-tag mapping with new tag.
	public DatasourceTag addDatasourceTag(String datasourceId, Tag tag) throws DalException {
		try {
			if (validateDatasource(datasourceId)) {
				// Transaction ops to wrap tag insertion & mapping insertion.
				m_transactionManager.startTransaction("fxhermesmetadb");
				m_tagDao.insert(tag);
				
				DatasourceTag datasourceTag = new DatasourceTag();
				datasourceTag.setDatasourceId(datasourceId);
				datasourceTag.setTagId(tag.getId());
				
				m_datasourceTagDao.insert(datasourceTag);
				m_transactionManager.commitTransaction();
				return datasourceTag;
			} 
		} catch (DalException e) {
			m_transactionManager.rollbackTransaction();
			log.error("Failed to add datasource tag mapping: {}, {} ", datasourceId, tag, e);
			return null;
		}
		
		throw new DalException(String.format("Can NOT find datasouce: %s", datasourceId));
	}
	
	// Add datasource-tag mapping with tag id.
	public DatasourceTag addDatasourceTag(String datasourceId, long tagId) throws DalException {
		try {
			if (validateDatasource(datasourceId) && validateTag(tagId)) {
				DatasourceTag datasourceTag = new DatasourceTag();
				datasourceTag.setDatasourceId(datasourceId);
				datasourceTag.setTagId(tagId);
				m_datasourceTagDao.insert(datasourceTag);
				return datasourceTag;
			}
		} catch (DalException e) {
			log.error("Failed to add datasource tag mapping: {}, {} ", datasourceId, tagId, e);
			return null;
		}
		
		throw new DalException(String.format("Can NOT find datasouce/tag: %s, %d", datasourceId, tagId));
	}
	
	// Remove datasource-tag mapping with datasource id & tag id.
	public DatasourceTag removeDatasourceTag(String datasourceId, long tagId) throws DalException {
		try {
			if (validateDatasource(datasourceId) && validateTag(tagId)) {
				DatasourceTag datasourceTag = new DatasourceTag();
				datasourceTag.setDatasourceId(datasourceId);
				datasourceTag.setTagId(tagId);
				
				// Transaction ops to wrap mapping deletion & tag deletion if necessary.
				m_transactionManager.startTransaction("fxhermesmetadb");
				m_datasourceTagDao.deleteByPK(datasourceTag);
				if (m_datasourceTagDao.countByTag(tagId, DatasourceTagEntity.READSET_COUNT).getCount() == 0) {
					Tag t = new Tag();
					t.setId(tagId);
					m_tagDao.deleteByPK(t);
				}
				m_transactionManager.commitTransaction();
				return datasourceTag;
			}
		} catch (DalException e) {
			m_transactionManager.rollbackTransaction();
			log.error("Failed to remove datasource tag mapping: {}, {} ", datasourceId, tagId, e);
			return null;
		}
		
		throw new DalException(String.format("Can NOT find datasouce/tag: %s, %d", datasourceId, tagId));
	}
	
	public boolean validateDatasource(String datasourceId) throws DalException {
		try {
			return m_datasourceDao.findByPK(datasourceId) != null;
		} catch (DalException e) {
			if (e.getCause() instanceof DalNotFoundException) {
				return false;
			}
			throw e;
		}
	}
	
	public boolean validateTag(long tagId) throws DalException {
		try {
			return m_tagDao.findByPK(tagId) != null;
		} catch (DalException e) {
			if (e.getCause() instanceof DalNotFoundException) {
				return false;
			}
			throw e;
		} 
	}

}
