package nherald.indigo.store.uow;

@FunctionalInterface
public interface Consumer<T>
{
    void run(T t);
}
