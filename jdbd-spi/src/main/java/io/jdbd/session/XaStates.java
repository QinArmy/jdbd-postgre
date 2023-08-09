package io.jdbd.session;

public enum XaStates {

    NONE,
    STARTED,
    ENDED,

    PREPARED;


    @Override
    public final String toString() {
        return String.format("%s.%s", XaStates.class.getName(), this.name());
    }


}
