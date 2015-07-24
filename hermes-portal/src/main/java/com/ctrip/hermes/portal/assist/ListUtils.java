package com.ctrip.hermes.portal.assist;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ListUtils {
	public static <T> List<T> getTopK(int k, Comparator<T> comparator, @SuppressWarnings("unchecked") List<T>... lists) {
		List<T> ret = new ArrayList<>();
		int[] indexs = new int[lists.length];
		for (int i = 0; i < k; i++) {
			T top = getNextTop(lists, indexs, comparator);
			if (top == null)
				break;
			ret.add(top);
		}
		return ret;
	}

	private static <T> T getNextTop(List<T>[] lists, int[] idxs, Comparator<T> comp) {
		int maxi = -1;
		for (int i = 0; i < lists.length; i++) {
			if (idxs[i] < lists[i].size()) {
				maxi = maxi < 0 || comp.compare(lists[maxi].get(idxs[maxi]), lists[i].get(idxs[i])) < 0 ? i : maxi;
			}
		}
		return maxi < 0 ? null : lists[maxi].get(idxs[maxi]++);
	}
}
