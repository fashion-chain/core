package org.fok.core.exception;

import onight.tfw.ojpa.api.exception.NotSuportException;

public class FokDposNodeNotReadyException extends NotSuportException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FokDposNodeNotReadyException(String message) {
		super(message);
	}

}
