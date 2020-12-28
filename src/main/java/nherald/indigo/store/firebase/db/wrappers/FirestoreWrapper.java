package nherald.indigo.store.firebase.db.wrappers;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;

import nherald.indigo.store.firebase.db.FirebaseBatch;
import nherald.indigo.store.firebase.db.FirebaseDatabase;
import nherald.indigo.store.firebase.db.FirebaseDocument;
import nherald.indigo.store.firebase.db.FirebaseDocumentId;

public class FirestoreWrapper implements FirebaseDatabase
{
    private final Firestore database;

    public FirestoreWrapper(Firestore database)
    {
        this.database = database;
    }

    @Override
    public FirebaseDocument get(FirebaseDocumentId id)
        throws InterruptedException, ExecutionException
    {
        final ApiFuture<DocumentSnapshot> future = asRef(id).get();

        return new DocumentSnapshotWrapper(future.get());
    }

    @Override
    public List<FirebaseDocument> getAll(List<FirebaseDocumentId> ids)
        throws InterruptedException, ExecutionException
    {
        final DocumentReference[] docRefs = asRefs(ids);

        final ApiFuture<List<DocumentSnapshot>> future = database.getAll(docRefs);

        return future.get()
            .stream()
            .map(DocumentSnapshotWrapper::new)
            .collect(Collectors.toList());
    }

    @Override
    public FirebaseBatch batch()
    {
        return new WriteBatchWrapper(database.batch(), this);
    }

    private DocumentReference[] asRefs(List<FirebaseDocumentId> ids)
    {
        return ids.stream()
            .map(this::asRef)
            .toArray(DocumentReference[]::new);
    }

    DocumentReference asRef(FirebaseDocumentId id)
    {
        return database.collection(id.getCollection())
            .document(id.getId());
    }
}
