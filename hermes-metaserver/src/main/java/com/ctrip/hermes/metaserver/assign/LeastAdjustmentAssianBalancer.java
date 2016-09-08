package com.ctrip.hermes.metaserver.assign;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.unidal.lookup.annotation.Named;

@Named(type = AssignBalancer.class)
public class LeastAdjustmentAssianBalancer implements AssignBalancer {

	public <T> Map<String, List<T>> assign(Map<String, List<T>> originAssigns, List<T> freeAssigns) {
		if (originAssigns == null || originAssigns.isEmpty()) {
			return new HashMap<String, List<T>>();
		}

		LinkedList<T> linkedFreeAssigns = (freeAssigns == null) ? new LinkedList<T>() : new LinkedList<T>(freeAssigns);

		// is Integer enough?
		int total = linkedFreeAssigns.size();
		Map<String, List<T>> newAssigns = new HashMap<String, List<T>>();

		for (Map.Entry<String, List<T>> assign : originAssigns.entrySet()) {
			LinkedList<T> newAssignList = new LinkedList<T>();
			if (assign.getValue() == null) {
				newAssigns.put(assign.getKey(), newAssignList);
			} else {
				newAssignList.addAll(assign.getValue());
				total += assign.getValue().size();
			}
			newAssigns.put(assign.getKey(), newAssignList);
		}

		int avg = total / originAssigns.size();
		int remainder = total % originAssigns.size();
		int countEqualsAvgPlus1 = 0;

		for (Map.Entry<String, List<T>> assign : newAssigns.entrySet()) {
			int originSize = assign.getValue().size();
			if (originSize > avg) {
				int targetSize = avg;
				if (countEqualsAvgPlus1 < remainder) {
					countEqualsAvgPlus1++;
					targetSize = avg + 1;
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
				int targetSize = avg;
				if (countEqualsAvgPlus1 < remainder) {
					targetSize = avg + 1;
					countEqualsAvgPlus1++;
				}
				for (int i = 0; i < targetSize - originSize; i++) {
					assign.getValue().add(linkedFreeAssigns.removeFirst());
				}

				if (linkedFreeAssigns.isEmpty()) {
					break;
				}
			}
		}

		return newAssigns;
	}
}