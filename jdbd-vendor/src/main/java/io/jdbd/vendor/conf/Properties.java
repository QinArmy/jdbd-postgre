package io.jdbd.vendor.conf;

import io.jdbd.config.PropertyException;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Properties<K extends IPropertyKey> {

    /**
     * @return actual property pair count( not contain {@link System#getProperties()} and {@link System#getenv()})
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
    String get(K key);

    /**
     * Return the property value associated with the given key,
     * or {@code null} if the key cannot be resolved.
     *
     * @param key the property name to resolve
     * @see #getProperty(String, String)
     * @see #getProperty(String, Class)
     * @see #getRequiredProperty(String)
     */
    String get(K key, String defaultValue);

    /**
     * Return the property value associated with the given key,
     * or {@code null} if the key cannot be resolved.
     *
     * @param key        the property name to resolve
     * @param targetType the expected type of the property value
     * @see #getRequiredProperty(String, Class)
     */
    @Nullable
    <T> T get(K key, Class<T> targetType) throws PropertyException;

    /**
     * Return the property value associated with the given key,
     * or {@code defaultValue} if the key cannot be resolved.
     *
     * @param key          the property name to resolve
     * @param targetType   the expected type of the property value
     * @param defaultValue the default value to return if no value is found
     * @see #getRequiredProperty(String, Class)
     */
    <T> T get(K key, Class<T> targetType, T defaultValue) throws PropertyException;

    /**
     * Return the property value associated with the given key,but not {@link String} ,the the property value showSQL:
     * {@code value1,value2,...,valuen}
     * or empty list the key cannot be resolved.
     *
     * @param key the property name to resolve
     * @return a  list
     * @see #getRequiredProperty(String, Class)
     */
    List<String> getList(K key);

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
    <T> List<T> getList(K key, Class<T> targetArrayType) throws PropertyException;

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
    <T> List<T> getList(K key, Class<T> targetArrayType, List<T> defaultList) throws PropertyException;

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
    <T> Set<T> getSet(K key, Class<T> targetArrayType) throws PropertyException;

    Set<String> getSet(K key);

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
    <T> Set<T> getSet(K key, Class<T> targetArrayType, Set<T> defaultSet) throws PropertyException;

    /**
     * Return the property value associated with the given key (never {@code null}).
     *
     * @throws IllegalStateException if the key cannot be resolved
     * @see #getRequiredProperty(String, Class)
     */
    String getNonNull(K key) throws PropertyException;

    /**
     * Return the property value associated with the given key, converted to the given
     * targetType (never {@code null}).
     *
     * @throws IllegalStateException if the given key cannot be resolved
     */
    <T> T getNonNull(K key, Class<T> targetType) throws PropertyException;

    String getOrDefault(K key) throws PropertyException;

    <T> T getOrDefault(K key, Class<T> targetType) throws PropertyException;


}
