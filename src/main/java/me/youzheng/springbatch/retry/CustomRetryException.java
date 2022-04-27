package me.youzheng.springbatch.retry;

public class CustomRetryException extends RuntimeException {

    public CustomRetryException() {
        super();
    }

    public CustomRetryException(String message) {
        super(message);
    }
}
