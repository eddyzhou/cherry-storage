package io.cherry.storage;

public class StorageException extends Exception {

	private static final long serialVersionUID = -2174642754318485884L;

	public StorageException(String msg) {
		super(msg);
	}

	public StorageException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
