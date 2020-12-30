package nherald.indigo.uow;

@FunctionalInterface
public interface Consumer<T>
{
    void run(T t);
}
