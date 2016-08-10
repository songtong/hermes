package com.ctrip.hermes.protal.util;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.unidal.tuple.Pair;

import com.ctrip.hermes.meta.entity.Meta;

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
	 * BeanDiffer.compareUsingAppropriateMethod() builder.differs().register(new
	 * DifferFactory(){...});
	 */

	private Field referenceKeyField;
	private Field accessorField;

	public MetaDiffer() throws Exception {
		referenceKeyField = MapEntryAccessor.class.getDeclaredField("referenceKey");
		referenceKeyField.setAccessible(true);

		accessorField = DiffNode.class.getDeclaredField("accessor");
		accessorField.setAccessible(true);
	}

	/**
	 * return Pair of
	 * 
	 * 1. "modified"(added, removed, changed) non-topics properties of Meta and
	 * 
	 * 2. "modified"(added, removed, changed) topics of Meta
	 * 
	 * can further find topic in oldMeta and newMeta to determine whether topic
	 * is added(in newMeta only), removed(in oldMeta only) or changed(in both
	 * meta)
	 */
	public Pair<Set<String>, Set<String>> compare(Meta oldMeta, Meta newMeta) throws Exception {

		ObjectDifferBuilder builder = ObjectDifferBuilder.startBuilding();
		DiffNode root = builder.build().compare(newMeta, oldMeta);

		final boolean showAll = false;

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

				if (state == State.CHANGED && !node.hasChildren()) {
					changed.add(node);
				}
			}
		};
		root.visit(visitor);

		/**
		 * version id
		 * 
		 * topics apps codecs endpoints storages servers
		 */

		List<DiffNode> all = new LinkedList<>();
		all.addAll(added);
		all.addAll(removed);
		all.addAll(changed);

		Set<String> nonTopicPropertiesToShow = new HashSet<>();
		Set<String> topicsToShow = new HashSet<>();

		for (DiffNode node : all) {
			if (is(node, "version", "id", "apps", "codecs", "endpoints", "storages", "servers")) {
				DiffNode lastParent = lastParentNode(node);
				nonTopicPropertiesToShow.add(lastParent.getPropertyName());
			}

			if (is(node, "topics")) {
				topicsToShow.add(findTopicName(node));
			}
		}

		return new Pair<>(nonTopicPropertiesToShow, topicsToShow);
	}

	private String findTopicName(DiffNode node) throws Exception {
		while (true) {
			// /topics{topic.name}
			if (node.getParentNode().getParentNode().isRootNode()) {
				MapEntryAccessor accessor = (MapEntryAccessor) getAccessor(node);
				return (String) referenceKeyField.get(accessor);
			} else {
				node = node.getParentNode();
			}
		}
	}

	private Accessor getAccessor(DiffNode node) {
		try {
			return (Accessor) accessorField.get(node);
		} catch (Exception e) {
			throw new RuntimeException("error get accessor field of DiffNode", e);
		}
	}

	private DiffNode lastParentNode(DiffNode node) {
		while (!node.getParentNode().isRootNode()) {
			node = node.getParentNode();
		}
		return node;
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

}
