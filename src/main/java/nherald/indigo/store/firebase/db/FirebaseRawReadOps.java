package nherald.indigo.store.firebase.db;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface FirebaseRawReadOps
{
    FirebaseRawDocument get(FirebaseRawDocumentId id)
        throws InterruptedException, ExecutionException;

    List<FirebaseRawDocument> getAll(List<FirebaseRawDocumentId> ids)
        throws InterruptedException, ExecutionException;
}
