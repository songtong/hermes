package com.ctrip.hermes.protal.util;

import javax.ws.rs.core.Response.Status;

public class ResponseUtils {
	static class ResponseWrapper {
		private Status status;
		private Object[] data;
		private String message;
		
		public ResponseWrapper(Status status, String message) {
			this.status = status;
			this.message = message;
		}
		public ResponseWrapper(Status status, String message, Object data) {
			this(status, message);
			this.data = new Object[]{data};
		}
		public ResponseWrapper(Status status, String message, Object[] data) {
			this(status, message);
			this.data = data;
		}
		public Status getStatus() {
			return status;
		}
		public void setStatus(Status status) {
			this.status = status;
		}
		public Object[] getData() {
			return data;
		}
		public void setData(Object[] data) {
			this.data = data;
		}
		public String getMessage() {
			return message;
		}
		public void setMessage(String message) {
			this.message = message;
		}
	}
	
	static class PaginationResponseWrapper extends ResponseWrapper {
		private int count;

		public PaginationResponseWrapper(Status status, String message, Object[] data, int count) {
			super(status, message, data);
			this.count = count;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
	}
	
	public static Object wrapResponse(Status status, String message, Object[] data) {
		return new ResponseWrapper(status, message, data);
	}
	
	public static Object wrapResponse(Status status, String message, Object data) {
		return new ResponseWrapper(status, message, data);
	}
	
	public static Object wrapResponse(Status status, Object[] data) {
		return new ResponseWrapper(status, null, data);
	}
	
	public static Object wrapResponse(Status status, Object data) {
		return new ResponseWrapper(status, null, data);
	}
	
	public static Object wrapPaginationResponse(Status status, String message, Object[] data, int count) {
		return new PaginationResponseWrapper(status, message, data, count);
	}

}
