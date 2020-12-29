package nherald.indigo.store.firebase.db.wrappers;

import com.google.cloud.firestore.Transaction;

import nherald.indigo.store.firebase.db.FirebaseRawTransaction;
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
