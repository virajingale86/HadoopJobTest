package com.hadoop.exception;

public class HadoopException extends Exception{

	private static final long serialVersionUID = -8597479051854474620L;

	public HadoopException() {
		super();
	}

	public HadoopException(String message){
		super(message);
	}
	
	public HadoopException(String message, Throwable e){
		super(message, e);
	}

}
