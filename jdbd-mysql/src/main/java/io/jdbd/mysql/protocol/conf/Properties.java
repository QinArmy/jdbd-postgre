package io.jdbd.mysql.protocol.conf;

import org.qinarmy.env.Environment;
import org.qinarmy.lang.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Properties extends Environment {

    /**
     * @return actual property pair count.
     */
    int size();

    /**
     * @return a unmodifiable map
     */
    Map<String, String> getSource();

    /**
     * Return the property value associated with the given key,
     * or {@code null} if the key cannot be resolved.
     *
     * @param key the property name to resolve
     * @see #getProperty(String, String)
     * @see #getProperty(String, Class)
     * @see #getRequiredProperty(String)
     */
    @Nullable
    String getProperty(PropertyKey key);

    /**
     * Return the property value associated with the given key,
     * or {@code null} if the key cannot be resolved.
     *
     * @param key the property name to resolve
     * @see #getProperty(String, String)
     * @see #getProperty(String, Class)
     * @see #getRequiredProperty(String)
     */
    String getProperty(PropertyKey key, String defaultValue);

    /**
     * Return the property value associated with the given key,
     * or {@code null} if the key cannot be resolved.
     *
     * @param key        the property name to resolve
     * @param targetType the expected type of the property value
     * @see #getRequiredProperty(String, Class)
     */
    @Nullable
    <T> T getProperty(PropertyKey key, Class<T> targetType);

    /**
     * Return the property value associated with the given key,
     * or {@code defaultValue} if the key cannot be resolved.
     *
     * @param key          the property name to resolve
     * @param targetType   the expected type of the property value
     * @param defaultValue the default value to return if no value is found
     * @see #getRequiredProperty(String, Class)
     */
    <T> T getProperty(PropertyKey key, Class<T> targetType, T defaultValue);

    /**
     * Return the property value associated with the given key,but not {@link String} ,the the property value showSQL:
     * {@code value1,value2,...,valuen}
     * or empty list the key cannot be resolved.
     *
     * @param key the property name to resolve
     * @return a  list
     * @see #getRequiredProperty(String, Class)
     */
    List<String> getPropertyList(PropertyKey key);

    /**
     * Return the property value associated with the given key,but not {@link String} ,the the property value showSQL:
     * {@code value1,value2,...,valuen}
     * or empty list the key cannot be resolved.
     *
     * @param key             the property name to resolve
     * @param targetArrayType the expected type of the property value
     * @return a  list
     * @see #getRequiredProperty(String, Class)
     */
    <T> List<T> getPropertyList(PropertyKey key, Class<T> targetArrayType);

    /**
     * Return the property value associated with the given key,
     * or {@link Collections#emptyList()} if the key cannot be resolved.
     *
     * @param key             the property name to resolve
     * @param targetArrayType the expected type of the property value
     * @param defaultList     the default list to return if no value is found
     * @return a  list
     * @see #getRequiredProperty(String, Class)
     */
    <T> List<T> getPropertyList(PropertyKey key, Class<T> targetArrayType, List<T> defaultList);

    /**
     * Return the property value associated with the given key,but not {@link String} ,the the property value showSQL:
     * {@code value1,value2,...,valuen}
     * or empty set the key cannot be resolved.
     *
     * @param key             the property name to resolve
     * @param targetArrayType the expected type of the property value
     * @return a  list
     * @see #getRequiredProperty(String, Class)
     */
    <T> Set<T> getPropertySet(PropertyKey key, Class<T> targetArrayType);

    Set<String> getPropertySet(PropertyKey key);

    /**
     * Return the property value associated with the given key,but not {@link String} ,the the property value showSQL:
     * {@code value1,value2,...,valuen}
     * or empty set the key cannot be resolved.
     *
     * @param key             the property name to resolve
     * @param targetArrayType the expected type of the property value
     * @param defaultSet      the default set to return if no value is found
     * @return a  list
     * @see #getRequiredProperty(String, Class)
     */
    <T> Set<T> getPropertySet(PropertyKey key, Class<T> targetArrayType, Set<T> defaultSet);

    /**
     * Return the property value associated with the given key (never {@code null}).
     *
     * @throws IllegalStateException if the key cannot be resolved
     * @see #getRequiredProperty(String, Class)
     */
    String getRequiredProperty(PropertyKey key) throws IllegalStateException;

    /**
     * Return the property value associated with the given key, converted to the given
     * targetType (never {@code null}).
     *
     * @throws IllegalStateException if the given key cannot be resolved
     */
    <T> T getRequiredProperty(PropertyKey key, Class<T> targetType) throws IllegalStateException;

}
