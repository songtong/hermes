package com.ctrip.hermes.admin.core.service.storage.exception;

public class StorageHandleErrorException extends Exception {
	/**
	 * 
	 */
   private static final long serialVersionUID = -5907881334860578054L;

	public StorageHandleErrorException(Throwable cause) {
		super(cause);
	}

	public StorageHandleErrorException(String message) {
		super(message);
	}
}
