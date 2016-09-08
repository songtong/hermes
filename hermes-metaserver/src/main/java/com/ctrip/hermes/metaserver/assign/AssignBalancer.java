package com.ctrip.hermes.metaserver.assign;

import java.util.List;
import java.util.Map;

public interface AssignBalancer {
	public <T> Map<String, List<T>> assign(Map<String, List<T>> originAssigns, List<T> freeAssigns);
}
