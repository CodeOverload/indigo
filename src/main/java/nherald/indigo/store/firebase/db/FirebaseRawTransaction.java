package nherald.indigo.store.firebase.db;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface FirebaseRawTransaction
{
    FirebaseDocument get(FirebaseDocumentId id)
        throws InterruptedException, ExecutionException;

    List<FirebaseDocument> getAll(List<FirebaseDocumentId> ids)
        throws InterruptedException, ExecutionException;

    <T> void set(FirebaseDocumentId id, T entity);

    void delete(FirebaseDocumentId id);
}
