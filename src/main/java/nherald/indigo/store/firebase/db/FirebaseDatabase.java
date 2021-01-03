package nherald.indigo.store.firebase.db;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import nherald.indigo.store.uow.Consumer;

public interface FirebaseDatabase
{
    FirebaseDocument get(FirebaseDocumentId id)
        throws InterruptedException, ExecutionException;

    List<FirebaseDocument> getAll(List<FirebaseDocumentId> ids)
        throws InterruptedException, ExecutionException;

    Collection<FirebaseDocumentId> list(String collectionId);

    void transaction(Consumer<FirebaseRawTransaction> runnable)
        throws InterruptedException, ExecutionException;
}
