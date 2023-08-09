package io.jdbd.result;

/**
 * @see RefCursor
 */
public enum CursorDirection {

    NEXT,
    PRIOR,
    FIRST,
    LAST,

    /**
     * must specified count
     */
    ABSOLUTE,

    /**
     * must specified count
     */
    RELATIVE,

    /**
     * must specified count
     */
    FORWARD,
    FORWARD_ALL,

    /**
     * must specified count
     */
    BACKWARD,
    BACKWARD_ALL

}
