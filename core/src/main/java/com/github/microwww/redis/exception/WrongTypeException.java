package com.github.microwww.redis.exception;

/**
 * Thrown when an operation is performed on a key holding the wrong data type.
 * For example: executing GET command on a SortedSet key.
 *
 * Redis standard error message:
 * WRONGTYPE Operation against a key holding the wrong kind of value
 */
public class WrongTypeException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;
    
    public static final String MESSAGE = "WRONGTYPE Operation against a key holding the wrong kind of value";
    
    public WrongTypeException() {
        super(MESSAGE);
    }
    
    public WrongTypeException(String message) {
        super(message);
    }
}
