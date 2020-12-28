package nherald.indigo.store.firebase.db;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface FirebaseDatabase
{
    FirebaseDocument get(FirebaseDocumentId id)
        throws InterruptedException, ExecutionException;

    List<FirebaseDocument> getAll(List<FirebaseDocumentId> ids)
        throws InterruptedException, ExecutionException;

    FirebaseBatch batch();
}