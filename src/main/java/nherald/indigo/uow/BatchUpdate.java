package nherald.indigo.uow;

/**
 * Similar to the UOW pattern, but specifically for updates. These are batched up
 * and run in a single batch. This will be done in a single, atomic transaction if
 * the storage supports it; Firestore does, but the file store doesn't (the latter
 * is just for quicker testing locally).
 *
 * Note that as the name suggests, this batches updates. Meaning that if you modify
 * an entity after you've passed it in, but before it's been committed, those
 * changes may be persisted on commit. This is a trade off for performance
 */
public interface BatchUpdate
{
    /**
     * Commit the pending updates. Note that this object isn't intended to be reused,
     * so the pending changes aren't cleared after calling this
     */
    public void commit();
}
