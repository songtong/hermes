package com.ctrip.hermes.portal.service.application;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.metaservice.dal.CachedDatasourceDao;
import com.ctrip.hermes.metaservice.dal.CachedPartitionDao;
import com.ctrip.hermes.metaservice.model.Partition;
import com.ctrip.hermes.metaservice.model.PartitionEntity;
import com.ctrip.hermes.metaservice.view.TopicView;
import com.ctrip.hermes.portal.application.TopicApplication;
import com.ctrip.hermes.portal.dal.datasourcetag.DatasourceTag;
import com.ctrip.hermes.portal.dal.datasourcetag.DatasourceTagDao;
import com.ctrip.hermes.portal.dal.datasourcetag.DatasourceTagEntity;
import com.ctrip.hermes.portal.dal.tag.CachedTagDao;
import com.ctrip.hermes.portal.dal.tag.Tag;
import com.ctrip.hermes.portal.dal.tag.TagEntity;

@Named(type=PartitionStrategy.class, value="mysql")
public class MysqlPartitionStrategy extends PartitionStrategy {
	private static final Logger m_logger = LoggerFactory.getLogger(MysqlPartitionStrategy.class);
	
	public static final String DEFAULT_TAG_GROUP = "BU";
	public static final String DEFAULT_TAG_NAME = "default";

	@Inject
	private CachedDatasourceDao m_datasourceDao;
	
	@Inject
	private CachedPartitionDao m_partitionDao;
	
	@Inject
	private CachedTagDao m_tagDao;
	
	@Inject
	private DatasourceTagDao m_datasourceTagDao;
	
	@Override
	public void applyStrategy(TopicApplication application, TopicView topicView) {
		topicView.setEndpointType("broker");
		super.applyStrategy(application, topicView);
	}

	@Override
	protected Pair<String, String> getDefaultDatasource(TopicApplication application) throws DalException {
		Tag tag = null;
		try {
			tag = m_tagDao.findByNameGroup(application.getProductLine(), DEFAULT_TAG_GROUP, TagEntity.READSET_FULL);
		} catch (DalNotFoundException e) {
			// ignore.
		}
		
		Tag defaultTag = null;
		try {
			defaultTag = m_tagDao.findByNameGroup(DEFAULT_TAG_NAME, DEFAULT_TAG_GROUP, TagEntity.READSET_FULL);
		} catch (DalNotFoundException e) {
			m_logger.warn("No default group tag defined on group {}!", DEFAULT_TAG_GROUP);
		}
		
		if (tag == null) {
			tag = defaultTag;
			if (tag == null) {
				throw new RuntimeException("Strategy can NOT be applied without eligable tags!");
			}
		}

		List<DatasourceTag> datasourcesTags = m_datasourceTagDao.findByTag(tag.getId(), DatasourceTagEntity.READSET_FULL);
		
		if (datasourcesTags == null || datasourcesTags.size() == 0) {
			if (tag != defaultTag) {
				datasourcesTags = m_datasourceTagDao.findByTag(defaultTag.getId(), DatasourceTagEntity.READSET_FULL);
				
				if (datasourcesTags == null || datasourcesTags.size() == 0) {
					throw new RuntimeException(String.format("Default tag on group %s can NOT find any mapping datasource!", DEFAULT_TAG_GROUP));
				}
			}
		}
		
		if (datasourcesTags.size() == 1) {
			return Pair.from(datasourcesTags.get(0).getDatasourceId(), datasourcesTags.get(0).getDatasourceId());
		}
		
		int partitionsCount = -1;
		String datasource = null;
		for (DatasourceTag datasourceTag : datasourcesTags) {
			Partition p = m_partitionDao.countByDatasource(datasourceTag.getDatasourceId(), PartitionEntity.READSET_COUNT);
			if (p.getCount() > partitionsCount) {
				partitionsCount = p.getCount();
				datasource = datasourceTag.getDatasourceId();
			}
		}
		
		return Pair.from(datasource, datasource);
	}

}
