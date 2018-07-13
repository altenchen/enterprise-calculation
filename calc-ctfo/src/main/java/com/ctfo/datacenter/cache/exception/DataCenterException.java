package com.ctfo.datacenter.cache.exception;

public class DataCenterException
    extends Exception {
    private static final long serialVersionUID = 1L;
    private String exceptionCode = "999";

    public DataCenterException() {
    }

    public DataCenterException(String message) {
        super(message);
    }

    public String getExceptionCode() {
        return this.exceptionCode;
    }

    public void setExceptionCode(String exceptionCode) {
        this.exceptionCode = exceptionCode;
    }
}
