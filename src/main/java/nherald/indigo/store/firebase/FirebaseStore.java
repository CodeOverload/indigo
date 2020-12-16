package nherald.indigo.store.firebase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.firebase.cloud.FirestoreClient;

import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.uow.BatchUpdate;

public class FirebaseStore implements Store
{
    private final Firestore database;

    public FirebaseStore()
    {
        database = FirestoreClient.getFirestore();
    }

    @Override
    public <T> T get(String namespace, String id, Class<T> itemType)
    {
        return get(namespace, Arrays.asList(id), itemType).get(0);
    }

    @Override
    public <T> List<T> get(String namespace, Collection<String> ids, Class<T> itemType)
    {
        try
        {
            final DocumentReference[] docsRefs = ids.stream()
                .map(id -> database.collection(namespace).document(id))
                .toArray(DocumentReference[]::new);

            ApiFuture<List<DocumentSnapshot>> future = database.getAll(docsRefs);

            return future.get()
                .stream()
                .map(doc -> {
                    if (!doc.exists()) return null;

                    return doc.toObject(itemType);
                })
                .collect(Collectors.toList());
        }
        catch (InterruptedException | ExecutionException ex)
        {
            throw new StoreException(String.format("Error running query for %s/%s", namespace, String.join(",", ids)), ex);
        }
    }

    @Override
    public boolean exists(String namespace, String id)
    {
        try
        {
            DocumentReference doc = database.collection(namespace).document(id);
            ApiFuture<DocumentSnapshot> future = doc.get();

            DocumentSnapshot document = future.get();
            return document.exists();
        }
        catch (InterruptedException | ExecutionException ex)
        {
            throw new StoreException(String.format("Error running query for %s/%s", namespace, id), ex);
        }
    }

    @Override
    public <T> void put(String namespace, String id, T entity, BatchUpdate batch)
    {
        ((FirebaseBatchUpdate) batch).put(namespace, id, entity);
    }

    @Override
    public void delete(String namespace, String id, BatchUpdate batch)
    {
        ((FirebaseBatchUpdate) batch).delete(namespace, id);
    }

    @Override
    public BatchUpdate startBatch()
    {
        return new FirebaseBatchUpdate(database);
    }
}
