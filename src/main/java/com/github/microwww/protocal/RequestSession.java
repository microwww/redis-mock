package com.github.microwww.protocal;

public class RequestSession {
    public static final int DEFAULT_DATABASE = 0;
    private int database;

    public RequestSession() {
        database = DEFAULT_DATABASE;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }
}
