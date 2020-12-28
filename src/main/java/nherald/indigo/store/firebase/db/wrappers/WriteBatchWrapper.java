package nherald.indigo.store.firebase.db.wrappers;

import java.util.concurrent.ExecutionException;

import com.google.cloud.firestore.WriteBatch;

import nherald.indigo.store.firebase.db.FirebaseBatch;
import nherald.indigo.store.firebase.db.FirebaseDocumentId;

public class WriteBatchWrapper implements FirebaseBatch
{
    private final WriteBatch batch;
    private final FirestoreWrapper database;

    public WriteBatchWrapper(WriteBatch batch, FirestoreWrapper database)
    {
        this.batch = batch;
        this.database = database;
    }

    @Override
    public <T> void set(FirebaseDocumentId id, T entity)
    {
        batch.set(database.asRef(id), entity);
    }

    @Override
    public void delete(FirebaseDocumentId id)
    {
        batch.delete(database.asRef(id));
    }

    @Override
    public void commit() throws InterruptedException, ExecutionException
    {
        batch.commit().get();
    }
}
