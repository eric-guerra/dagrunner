package com.ericguerra.dag.model;

public class InvalidDagException extends RuntimeException {
    public InvalidDagException(String message) {
        super(message);
    }
}
