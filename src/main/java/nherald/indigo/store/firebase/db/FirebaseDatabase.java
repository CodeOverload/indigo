package nherald.indigo.store.firebase.db;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import nherald.indigo.store.uow.Consumer;

public interface FirebaseDatabase extends FirebaseReadOps
{
    Collection<FirebaseDocumentId> list(String collectionId);

    void transaction(Consumer<FirebaseRawTransaction> runnable)
        throws InterruptedException, ExecutionException;
}
