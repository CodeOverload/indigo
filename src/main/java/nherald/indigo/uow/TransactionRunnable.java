package nherald.indigo.uow;

@FunctionalInterface
public interface TransactionRunnable
{
    void run(Transaction t);
}
