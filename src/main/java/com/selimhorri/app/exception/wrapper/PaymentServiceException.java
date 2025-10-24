package com.selimhorri.app.exception.wrapper;

public class PaymentServiceException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;
	
	public PaymentServiceException() {
		super();
	}
	
	public PaymentServiceException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public PaymentServiceException(String message) {
		super(message);
	}
	
	public PaymentServiceException(Throwable cause) {
		super(cause);
	}
	
	
	
}










