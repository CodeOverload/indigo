package nherald.indigo.store.firebase.db;

import java.util.concurrent.ExecutionException;

public interface FirebaseBatch
{
    <T> void set(FirebaseDocumentId id, T entity);

    void delete(FirebaseDocumentId id);

    void commit() throws InterruptedException, ExecutionException;
}
