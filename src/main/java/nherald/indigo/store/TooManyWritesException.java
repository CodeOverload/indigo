package nherald.indigo.store;

/**
 * Thrown when the number of writes in a transaction/batch exceeds the maximum
 * permitted by the storage implementation. Not all implementations enforce a
 * limit, Firestore has a limit of 500 write operations
 */
public class TooManyWritesException extends StoreException
{
    private static final long serialVersionUID = 1L;

    public TooManyWritesException(String message)
    {
        super(message);
    }
}
