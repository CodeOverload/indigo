package nherald.indigo.uow;

/**
 * A single transaction. Different Store implementations have different
 * transaction support; e.g. Firestore supports transactions fully (they're run
 * as an atomic operation, and if any of the objects read are updated by
 * another process in the meantime the transaction is retried). The File
 * Store implementation doesn't currently support transactions.
 *
 * Note that implementations may batch updates. Meaning that if you modify
 * an entity after you've passed it in, but before it's been committed, those
 * changes may be persisted on commit. This is a trade off for performance
 */
public interface Transaction
{
    /**
     * Commit the pending updates. Note that this object isn't intended to be reused,
     * so the pending changes aren't cleared after calling this
     */
    public void commit();
}
