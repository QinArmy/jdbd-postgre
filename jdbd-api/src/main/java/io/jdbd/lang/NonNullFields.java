package io.jdbd.lang;


import javax.annotation.Nonnull;
import javax.annotation.meta.TypeQualifierDefault;
import java.lang.annotation.*;

/**
 * A common jdbd annotation to declare that fields are to be considered as
 * non-nullable by default for a given package.
 *
 * <p>Leverages JSR-305 meta-annotations to indicate nullability in Java to common
 * tools with JSR-305 support and used by Kotlin to infer nullability of jdbd API.
 *
 * <p>Should be used at package level in association with {@link io.jdbd.lang.Nullable}
 * annotations at field level.
 *
 * <p> reference {@code @org.springframework.lang.NonNullFields}
 *
 * @since 1.0
 * @see io.jdbd.lang.NonNullApi
 * @see io.jdbd.lang.Nullable
 * @see io.jdbd.lang.NonNull
 */
@Target(ElementType.PACKAGE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Nonnull
@TypeQualifierDefault(ElementType.FIELD)
public @interface NonNullFields {

}
