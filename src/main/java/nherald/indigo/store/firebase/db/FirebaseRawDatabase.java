package nherald.indigo.store.firebase.db;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import nherald.indigo.store.uow.Consumer;

public interface FirebaseRawDatabase extends FirebaseRawReadOps
{
    Collection<FirebaseRawDocumentId> list(String collectionId);

    void transaction(Consumer<FirebaseRawTransaction> runnable)
        throws InterruptedException, ExecutionException;
}
