package com.ctrip.hermes.collector.pipe;

import com.ctrip.hermes.collector.pipeline.annotation.ProcessOn;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;

public abstract class RecordPipe extends Pipe<Record<?>>{

	public RecordPipe() {
		super(Record.class);
	}

	@Override
	public boolean validate(Record<?> record) {
		if (record != null) {
			// Validate the record type that can be handled.
			ProcessOn processOn = this.getClass().getAnnotation(ProcessOn.class);
			if (processOn == null) {
				return false;
			}
			
			for (RecordType type : processOn.value()) {
				if (type == record.getType()) {
					return true;
				}
			}
		}
		return false;
	}
}

