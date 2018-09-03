package com.vigneshraja.sessionization.models;

/**
 * Created by vraja on 9/3/18
 */
public interface Event {

    /**
     * Returns the key that will be used to build sessions
     */
    String getKey();
}
