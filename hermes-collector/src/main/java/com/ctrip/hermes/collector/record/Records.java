package com.ctrip.hermes.collector.record;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tenglinxiao
 *
 */
@SuppressWarnings("rawtypes")
public class Records extends Record {
	private List<Record<?>> m_records = new ArrayList<Record<?>>();
	public Records() {
		super(null);
	}
	
	public List<Record<?>> getRecords() {
		return m_records;
	}
	
	public void addRecord(Record<?> record) {
		m_records.add(record);
	}
	
	public Record<?> getRecord(String id) {
		for (int index = 0; index < m_records.size(); index++) {
			if (m_records.get(index).getId().equals(id)) {
				return m_records.get(index);
			}
		}
		return null;
	}
	
	public int size() {
		return m_records.size();
	}

	@Override
	public long getTimestamp() {
		return -1;
	}
	

//	@Override
//	public void deserialize(JsonNode json) throws DeserializeException {
//		ArrayNode array = (ArrayNode)json;
//		JsonNode jsonNode = null;
//		Record<? extends RecordContent> record = null;
//		Iterator<JsonNode> iter = array.iterator();
//		while (iter.hasNext()) {
//			jsonNode = iter.next();
//			
//			// Determine it's TimeMomentCommand/TimeWindowCommand.
//			if (jsonNode.has("moment")) {
//				record = TimeMomentRecord.newTimeMomentCommand();
//			} else {
//				record = TimeWindowRecord.newTimeWindowCommand();
//			}
//			record.deserialize(jsonNode);
//			m_records.add(record);
//		}
//	}
//
//	@Override
//	public JsonNode serialize() throws SerializeException {
//		ArrayNode array = new ObjectMapper().createArrayNode();
//		for (Record<? extends RecordContent> r : m_records) {
//			array.add(r.serialize());
//		}
//		return array;
//	} 
}
