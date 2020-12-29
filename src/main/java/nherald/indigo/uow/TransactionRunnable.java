package nherald.indigo.uow;

@FunctionalInterface
public interface TransactionRunnable<T>
{
    void run(T t);
}
