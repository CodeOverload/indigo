package nherald.indigo.store.firebase;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import nherald.indigo.store.StoreException;
import nherald.indigo.store.StoreReadOps;
import nherald.indigo.store.firebase.db.FirebaseRawDocument;
import nherald.indigo.store.firebase.db.FirebaseRawDocumentId;
import nherald.indigo.store.firebase.db.FirebaseRawReadOps;

public abstract class FirebaseReadOps implements StoreReadOps
{
    private final FirebaseRawReadOps readOps;

    protected FirebaseReadOps(FirebaseRawReadOps readOps)
    {
        this.readOps = readOps;
    }

    @Override
    public <T> T get(String namespace, String id, Class<T> itemType)
    {
        return get(namespace, Arrays.asList(id), itemType).get(0);
    }

    @Override
    public <T> List<T> get(String namespace, List<String> ids, Class<T> itemType)
    {
        final List<FirebaseRawDocumentId> docIds = ids.stream()
            .map(id -> new FirebaseRawDocumentId(namespace, id))
            .collect(Collectors.toList());

        try
        {
            final List<FirebaseRawDocument> docs = readOps.getAll(docIds);

            return docs.stream()
                .map(doc -> {
                    if (!doc.exists()) return null;

                    return doc.asObject(itemType);
                })
                .collect(Collectors.toList());
        }
        catch (InterruptedException | ExecutionException ex)
        {
            throw new StoreException(String.format("Error getting %s/%s", namespace, String.join(",", ids)), ex);
        }
    }

    @Override
    public boolean exists(String namespace, String id)
    {
        final FirebaseRawDocumentId docId = new FirebaseRawDocumentId(namespace, id);

        try
        {
            final FirebaseRawDocument document = readOps.get(docId);

            return document.exists();
        }
        catch (InterruptedException | ExecutionException ex)
        {
            throw new StoreException(String.format("Error getting %s/%s", namespace, id), ex);
        }
    }
}
