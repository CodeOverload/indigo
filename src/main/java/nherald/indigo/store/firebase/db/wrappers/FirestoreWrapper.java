package nherald.indigo.store.firebase.db.wrappers;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;

import nherald.indigo.store.firebase.db.FirebaseRawTransaction;
import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.firebase.db.FirebaseRawDatabase;
import nherald.indigo.store.firebase.db.FirebaseRawDocument;
import nherald.indigo.store.firebase.db.FirebaseRawDocumentId;

public class FirestoreWrapper implements FirebaseRawDatabase
{
    private final Firestore database;

    public FirestoreWrapper(Firestore database)
    {
        this.database = database;
    }

    @Override
    public FirebaseRawDocument get(FirebaseRawDocumentId id)
        throws InterruptedException, ExecutionException
    {
        final ApiFuture<DocumentSnapshot> future = asRef(id).get();

        return new DocumentSnapshotWrapper(future.get());
    }

    @Override
    public List<FirebaseRawDocument> getAll(List<FirebaseRawDocumentId> ids)
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
    public Collection<FirebaseRawDocumentId> list(String collectionId)
    {
        final Iterable<DocumentReference> collectionRef
            = database.collection(collectionId).listDocuments();

        return StreamSupport.stream(collectionRef.spliterator(), false)
            .map(FirestoreWrapper::asId)
            .collect(Collectors.toList());
    }

    @Override
    public void transaction(Consumer<FirebaseRawTransaction> runnable)
        throws InterruptedException, ExecutionException
    {
        database.runTransaction(firebaseTransaction -> {
            final FirebaseRawTransaction transaction = new TransactionWrapper(firebaseTransaction, this);
            runnable.run(transaction);
            return null;
        }).get();
    }

    private DocumentReference[] asRefs(List<FirebaseRawDocumentId> ids)
    {
        return ids.stream()
            .map(this::asRef)
            .toArray(DocumentReference[]::new);
    }

    DocumentReference asRef(FirebaseRawDocumentId id)
    {
        return database.collection(id.getCollection())
            .document(id.getId());
    }

    private static FirebaseRawDocumentId asId(DocumentReference docRef)
    {
        return new FirebaseRawDocumentId(docRef.getParent().getId(),
            docRef.getId());
    }
}
