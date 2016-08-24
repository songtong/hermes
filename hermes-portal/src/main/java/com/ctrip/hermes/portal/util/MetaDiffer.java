package com.ctrip.hermes.portal.util;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.meta.entity.App;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Idc;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

import de.danielbechler.diff.ObjectDifferBuilder;
import de.danielbechler.diff.access.Accessor;
import de.danielbechler.diff.access.MapEntryAccessor;
import de.danielbechler.diff.node.DiffNode;
import de.danielbechler.diff.node.DiffNode.State;
import de.danielbechler.diff.node.DiffNode.Visitor;
import de.danielbechler.diff.node.Visit;
import de.danielbechler.diff.path.NodePath;

/**
 * @author marsqing
 *
 *         Aug 8, 2016 4:35:57 PM
 */
public class MetaDiffer {

	/**
	 * BeanDiffer.compareUsingAppropriateMethod() builder.differs().register(new DifferFactory(){...});
	 */

	private Field referenceKeyField;

	private Field accessorField;

	public MetaDiffer() {
		try {
			referenceKeyField = MapEntryAccessor.class.getDeclaredField("referenceKey");
			referenceKeyField.setAccessible(true);

			accessorField = DiffNode.class.getDeclaredField("accessor");
			accessorField.setAccessible(true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * return Pair of
	 * 
	 * 1. "modified"(added, removed, changed) non-topics properties of Meta and
	 * 
	 * 2. "modified"(added, removed, changed) topics of Meta
	 * 
	 * can further find topic in oldMeta and newMeta to determine whether topic is added(in newMeta only), removed(in oldMeta only)
	 * or changed(in both meta)
	 */
	public MetaDiff compare(Meta oldMeta, Meta newMeta) {

		ObjectDifferBuilder builder = ObjectDifferBuilder.startBuilding();
		DiffNode root = builder.build().compare(newMeta, oldMeta);

		final boolean showAll = false;

		MetaDiff metaDiff = new MetaDiff();

		final List<DiffNode> added = new LinkedList<>();
		final List<DiffNode> removed = new LinkedList<>();
		final List<DiffNode> changed = new LinkedList<>();

		Visitor visitor = new Visitor() {

			@Override
			public void node(DiffNode node, Visit visit) {
				if (showAll) {
					System.out.println(node.getPath() + " => " + node.getState());
				}

				State state = node.getState();

				if (state == State.ADDED) {
					added.add(node);
					visit.dontGoDeeper();
				}

				if (state == State.REMOVED) {
					removed.add(node);
					visit.dontGoDeeper();
				}

				if (state == State.CHANGED) {
					changed.add(node);
				}
			}
		};
		root.visit(visitor);

		for (DiffNode node : changed) {
			if (is(node, "topics")) {
				metaDiff.addChangedTopic(newMeta.getTopics().get(findTopicName(node)));
			} else {
				dispatchNonTopicNode(node, metaDiff, newMeta);
			}
		}

		for (DiffNode node : added) {
			if (is(node, "topics")) {
				String topicName = findTopicName(node);
				if (metaDiff.getChangedTopics() == null || metaDiff.getChangedTopics().get(topicName) == null) {
					metaDiff.addAddedTopic(newMeta.getTopics().get(findTopicName(node)));
				}
			} else {
				dispatchNonTopicNode(node, metaDiff, newMeta);
			}
		}
		for (DiffNode node : removed) {
			if (is(node, "topics")) {
				String topicName = findTopicName(node);
				if (metaDiff.getChangedTopics() == null || metaDiff.getChangedTopics().get(topicName) == null) {
					metaDiff.addRemovedTopic(oldMeta.getTopics().get(findTopicName(node)));
				}
			} else {
				dispatchNonTopicNode(node, metaDiff, newMeta);
			}
		}

		// return new Pair<>(nonTopicPropertiesToShow, topicsToShow);
		return metaDiff;
	}

	private String findTopicName(DiffNode node) {
		try {
			while (true) {
				// /topics{topic.name}
				if (node.getParentNode() == null || node.getParentNode().getParentNode() == null) {
					return null;
				}

				if (node.getParentNode().getParentNode().isRootNode()) {
					MapEntryAccessor accessor = (MapEntryAccessor) getAccessor(node);
					return (String) referenceKeyField.get(accessor);
				} else {
					node = node.getParentNode();
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Accessor getAccessor(DiffNode node) {
		try {
			return (Accessor) accessorField.get(node);
		} catch (Exception e) {
			throw new RuntimeException("error get accessor field of DiffNode", e);
		}
	}

	private void dispatchNonTopicNode(DiffNode node, MetaDiff metaDiff, Meta newMeta) {
		if (is(node, "version")) {
			metaDiff.setVersion(newMeta.getVersion());
		} else if (is(node, "id")) {
			metaDiff.setId(newMeta.getId());
		} else if (is(node, "apps")) {
			metaDiff.setApps(newMeta.getApps());
		} else if (is(node, "codecs")) {
			metaDiff.setCodecs(newMeta.getCodecs());
		} else if (is(node, "endpoints")) {
			metaDiff.setEndpoints(newMeta.getEndpoints());
		} else if (is(node, "storages")) {
			metaDiff.setStorages(newMeta.getStorages());
		} else if (is(node, "servers")) {
			metaDiff.setServers(newMeta.getServers());
		} else if (is(node, "idcs")) {
			metaDiff.setIdcs(newMeta.getIdcs());
		}
	}

	private boolean is(DiffNode node, String... properties) {
		NodePath curPath = node.getPath();
		for (String p : properties) {
			NodePath pathToCompare = NodePath.with(p);
			if (curPath.isChildOf(pathToCompare) || curPath.equals(pathToCompare)) {
				return true;
			}
		}

		return false;
	}

	public static class MetaDiff {
		private Map<String, Topic> addedTopics;

		private Map<String, Topic> removedTopics;

		private Map<String, Topic> changedTopics;

		private Map<Long, App> apps;

		private Map<String, Codec> codecs;

		private Map<String, Endpoint> endpoints;

		private Map<String, Idc> idcs;

		private Map<String, Server> servers;

		private Map<String, Storage> storages;

		private Long version;

		private Long id;

		public Map<String, Topic> getAddedTopics() {
			return addedTopics;
		}

		public void addAddedTopic(Topic topic) {
			if (topic == null) {
				return;
			}

			if (addedTopics == null) {
				addedTopics = new LinkedHashMap<String, Topic>();
			}
			addedTopics.put(topic.getName(), topic);
		}

		public Map<String, Topic> getRemovedTopics() {
			return removedTopics;
		}

		public void addRemovedTopic(Topic topic) {
			if (topic == null) {
				return;
			}
			if (removedTopics == null) {
				removedTopics = new LinkedHashMap<String, Topic>();
			}
			removedTopics.put(topic.getName(), topic);
		}

		public Map<String, Topic> getChangedTopics() {
			return changedTopics;
		}

		public void addChangedTopic(Topic topic) {
			if (topic == null) {
				return;
			}

			if (changedTopics == null) {
				changedTopics = new LinkedHashMap<String, Topic>();
			}
			changedTopics.put(topic.getName(), topic);
		}

		public Map<Long, App> getApps() {
			return apps;
		}

		public void setApps(Map<Long, App> apps) {
			this.apps = apps;
		}

		public Map<String, Codec> getCodecs() {
			return codecs;
		}

		public void setCodecs(Map<String, Codec> codecs) {
			this.codecs = codecs;
		}

		public Map<String, Endpoint> getEndpoints() {
			return endpoints;
		}

		public void setEndpoints(Map<String, Endpoint> endpoints) {
			this.endpoints = endpoints;
		}

		public Map<String, Idc> getIdcs() {
			return idcs;
		}

		public void setIdcs(Map<String, Idc> idcs) {
			this.idcs = idcs;
		}

		public Map<String, Server> getServers() {
			return servers;
		}

		public void setServers(Map<String, Server> servers) {
			this.servers = servers;
		}

		public Map<String, Storage> getStorages() {
			return storages;
		}

		public void setStorages(Map<String, Storage> storages) {
			this.storages = storages;
		}

		public Long getVersion() {
			return version;
		}

		public void setVersion(Long version) {
			this.version = version;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

	}

}
