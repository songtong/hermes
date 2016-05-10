package com.ctrip.hermes.admin.core.service;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.model.Kv;
import com.ctrip.hermes.admin.core.model.KvDao;
import com.ctrip.hermes.admin.core.model.KvEntity;
import com.ctrip.hermes.core.utils.StringUtils;

@Named(type = KVService.class)
public class DefaultKVService implements KVService {
	private static final Logger log = LoggerFactory.getLogger(DefaultKVService.class);

	@Inject
	private KvDao m_kvDao;

	private String getDBKey(String modelKey, Tag tag) {
		if (StringUtils.isBlank(modelKey)) {
			throw new IllegalArgumentException("Model key can not be blank.");
		}
		return tag.simple() + "_" + modelKey;
	}

	private String getModelKey(String dbKey, Tag tag) {
		if (StringUtils.isBlank(dbKey)) {
			throw new IllegalArgumentException("DB key can not be blank.");
		}
		String[] parts = dbKey.split("_", 2);
		return parts.length == 2 && parts[0].equals(tag.simple()) ? parts[1] : parts[0];
	}

	@Override
	public String getValue(String key) {
		return getValue(key, Tag.DEFAULT);
	}

	@Override
	public String getValue(String key, Tag tag) {
		if (key.length() > 200) {
			throw new IllegalArgumentException("Key's length must be less than 200 ...");
		}
		key = getDBKey(key, tag);
		try {
			List<Kv> kvs = m_kvDao.findByKey(key, KvEntity.READSET_FULL);
			if (kvs.size() > 0) {
				return kvs.get(0).getV();
			}
		} catch (Exception e) {
			log.error("Find key[{}]'s value failed.", key, e);
		}
		return null;
	}

	@Override
	public void setKeyValue(String key, String value) throws Exception {
		setKeyValue(key, value, Tag.DEFAULT);
	}

	@Override
	public void setKeyValue(String key, String value, Tag tag) throws Exception {
		key = getDBKey(key, tag);
		List<Kv> kvs = m_kvDao.findByKey(key, KvEntity.READSET_FULL);
		Kv kv = kvs.size() > 0 ? kvs.get(0) : null;
		if (kv == null) {
			m_kvDao.insert(new Kv().setK(key).setV(value).setTag(tag.name()).setCreationDate(new Date()));
		} else {
			kv.setV(value);
			kv.setDataChangeLastTime(new Date());
			m_kvDao.updateByPK(kv, KvEntity.UPDATESET_FULL);
		}
	}

	@Override
	public Map<Tag, Map<String, String>> list() {
		Map<Tag, Map<String, String>> m = new HashMap<>();
		try {
			for (Kv kv : m_kvDao.findAll(KvEntity.READSET_FULL)) {
				Tag tag = Tag.getByName(kv.getTag());
				Map<String, String> kvs = m.get(tag);
				if (kvs == null) {
					kvs = new HashMap<>();
					m.put(tag, kvs);
				}
				kvs.put(getModelKey(kv.getK(), tag), kv.getV());
			}
		} catch (Exception e) {
			log.error("List all KVs failed!", e);
		}
		return m;
	}

	@Override
	public Map<String, String> list(Tag tag) {
		Map<String, String> m = new HashMap<>();
		try {
			for (Kv kv : m_kvDao.findByTag(tag.name(), KvEntity.READSET_FULL)) {
				m.put(getModelKey(kv.getK(), tag), kv.getV());
			}
		} catch (Exception e) {
			log.error("List tag[{}] KVs failed!", tag, e);
		}
		return m;
	}
}
