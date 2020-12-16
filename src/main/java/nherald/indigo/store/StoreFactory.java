package nherald.indigo.store;

@FunctionalInterface
public interface StoreFactory
{
    Store get();
}
