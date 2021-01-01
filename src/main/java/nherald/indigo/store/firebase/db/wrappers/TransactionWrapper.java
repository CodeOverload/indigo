package nherald.indigo.store.firebase.db.wrappers;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Transaction;

import nherald.indigo.store.firebase.db.FirebaseRawTransaction;
import nherald.indigo.store.firebase.db.FirebaseDocument;
import nherald.indigo.store.firebase.db.FirebaseDocumentId;

public class TransactionWrapper implements FirebaseRawTransaction
{
    private final Transaction transaction;
    private final FirestoreWrapper database;

    public TransactionWrapper(Transaction transaction, FirestoreWrapper database)
    {
        this.transaction = transaction;
        this.database = database;
    }

    @Override
    public FirebaseDocument get(FirebaseDocumentId id)
        throws InterruptedException, ExecutionException
    {
        final ApiFuture<DocumentSnapshot> doc = transaction.get(database.asRef(id));

        return new DocumentSnapshotWrapper(doc.get());
    }

    @Override
    public List<FirebaseDocument> getAll(List<FirebaseDocumentId> ids)
        throws InterruptedException, ExecutionException
    {
        final DocumentReference[] docIds = ids.stream()
            .map(database::asRef)
            .toArray(DocumentReference[]::new);

        return transaction.getAll(docIds).get()
            .stream()
            .map(DocumentSnapshotWrapper::new)
            .collect(Collectors.toList());
    }

    @Override
    public <T> void set(FirebaseDocumentId id, T entity)
    {
        transaction.set(database.asRef(id), entity);
    }

    @Override
    public void delete(FirebaseDocumentId id)
    {
        transaction.delete(database.asRef(id));
    }
}
