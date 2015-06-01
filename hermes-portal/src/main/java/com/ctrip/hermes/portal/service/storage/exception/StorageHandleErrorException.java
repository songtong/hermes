package com.ctrip.hermes.portal.service.storage.exception;

public class StorageHandleErrorException extends Exception {
	public StorageHandleErrorException(Throwable cause) {
		super(cause);
	}

	public StorageHandleErrorException(String message) {
		super(message);
	}
}
