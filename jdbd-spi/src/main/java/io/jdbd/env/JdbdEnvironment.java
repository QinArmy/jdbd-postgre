package io.jdbd.env;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;

import java.util.Map;

/**
 * @see <a href="https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html">Specifying Database Connection URLs</a>
 */
@Deprecated
public interface JdbdEnvironment {

    String protocol();

    /**
     * @return empty or sub protocol
     */
    String subProtocol();


    /**
     * <p>
     * The attribute map that dont' contain password or private key.
     * </p>
     *
     * @return a unmodified map (readonly)
     */
    Map<String, Object> attributeMap();

    /**
     * <p>
     * The attribute map that dont' contain password or private key.
     * </p>
     *
     * @return a unmodified map (readonly)
     */
    Map<String, Object> propertyMap();

    /**
     * @throws JdbdException throw when <ul>
     *                       <li>converting failure</li>
     *                       <li>keyName is password or private key</li>
     *                       </ul>
     */
    @Nullable
    <T> T get(String keyName, Class<T> valueClass) throws JdbdException;

    /**
     * @throws JdbdException throw when <ul>
     *                       <li>converting failure</li>
     *                       <li>keyName is password or private key</li>
     *                       </ul>
     */
    <T> T getNonNull(String keyName, Class<T> valueClass) throws JdbdException;



}
