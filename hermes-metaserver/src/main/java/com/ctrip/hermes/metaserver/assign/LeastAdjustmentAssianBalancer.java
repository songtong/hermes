package com.ctrip.hermes.metaserver.assign;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Named;

@Named(type = AssignBalancer.class)
public class LeastAdjustmentAssianBalancer<T> implements AssignBalancer<T> {

	public Map<String, List<T>> assign(Map<String, List<T>> originAssigns, List<T> freeAssigns) {
		if (originAssigns == null || originAssigns.isEmpty()) {
			return new HashMap<String, List<T>>();
		}

		LinkedList<T> linkedFreeAssigns = null;
		if (freeAssigns == null || freeAssigns.isEmpty()) {
			linkedFreeAssigns = new LinkedList<>();
		} else {
			linkedFreeAssigns = new LinkedList<>(freeAssigns);
		}

		// is Integer enough?
		int total = linkedFreeAssigns.size();
		Map<String, List<T>> newAssigns = new HashMap<String, List<T>>();

		for (Map.Entry<String, List<T>> assign : originAssigns.entrySet()) {
			if (assign.getValue() == null) {
				LinkedList<T> newAssignList = new LinkedList<T>();
				newAssigns.put(assign.getKey(), newAssignList);
			} else {
				LinkedList<T> newAssignList = new LinkedList<T>(assign.getValue());
				newAssigns.put(assign.getKey(), newAssignList);
				total += assign.getValue().size();
			}
		}

		int avg = total / originAssigns.size();
		int remaining = total % originAssigns.size();
		int countEqualsAvgPlus1 = 0;

		for (Map.Entry<String, List<T>> assign : newAssigns.entrySet()) {
			int originSize = assign.getValue().size();
			if (originSize > avg) {
				int targetSize = avg;
				if (countEqualsAvgPlus1 < remaining) {
					countEqualsAvgPlus1++;
					targetSize++;
				}
				for (int i = 0; i < originSize - targetSize; i++) {
					linkedFreeAssigns.add(assign.getValue().remove(0));
				}
			}
		}
		

		if (!linkedFreeAssigns.isEmpty()) {
			Collections.shuffle(linkedFreeAssigns);
			for (Map.Entry<String, List<T>> assign : newAssigns.entrySet()) {
				int originSize = assign.getValue().size();
				if (countEqualsAvgPlus1 < remaining) {
					if (originSize < avg + 1) {
						countEqualsAvgPlus1++;
						for (int i = 0; i < avg + 1 - originSize; i++) {
							assign.getValue().add(linkedFreeAssigns.removeFirst());
						}
					}
				} else {
					if (originSize < avg) {
						for (int i = 0; i < avg - originSize; i++) {
							assign.getValue().add(linkedFreeAssigns.removeFirst());
						}
					}
				}
				if (linkedFreeAssigns.isEmpty()) {
					break;
				}
			}
		}

		return newAssigns;
	}
}