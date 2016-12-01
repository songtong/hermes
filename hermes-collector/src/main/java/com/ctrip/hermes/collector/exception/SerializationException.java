package com.ctrip.hermes.collector.exception;

public class SerializationException {
	public static class SerializeException extends Exception {
		public SerializeException(String message) {
			super(message);
		}
		
		public SerializeException(Throwable t) {
			super(t);
		}
		
		public SerializeException(String message, Throwable t) {
			super(message, t);
		}
	}
	
	public static class DeserializeException extends Exception {
		public DeserializeException(String message) {
			super(message);
		}
		
		public DeserializeException(Throwable t) {
			super(t);
		}
		
		public DeserializeException(String message, Throwable t) {
			super(message, t);
		}
	}

}
