package ir.sahab.zk.client;

import java.util.function.Supplier;

/**
 * Java interface for {@link Supplier} cannot throw checked exceptions. This is a supplier type which can throw a type
 * of checked exception.
 */
@FunctionalInterface
public interface SupplierWithException<T, E extends Throwable> {

    T get() throws E;
}