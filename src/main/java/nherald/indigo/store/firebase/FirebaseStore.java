package nherald.indigo.store.firebase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.firebase.db.FirebaseRawDatabase;
import nherald.indigo.store.firebase.db.FirebaseRawDocument;
import nherald.indigo.store.firebase.db.FirebaseRawDocumentId;
import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.store.uow.WrapTransaction;

public class FirebaseStore implements Store
{
    private final FirebaseRawDatabase database;

    public FirebaseStore(FirebaseRawDatabase database)
    {
        this.database = database;
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
            final List<FirebaseRawDocument> docs = database.getAll(docIds);

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
            final FirebaseRawDocument document = database.get(docId);

            return document.exists();
        }
        catch (InterruptedException | ExecutionException ex)
        {
            throw new StoreException(String.format("Error getting %s/%s", namespace, id), ex);
        }
    }

    @Override
    public Collection<String> list(String namespace)
    {
        return database.list(namespace)
            .stream()
            .map(FirebaseRawDocumentId::getId)
            .collect(Collectors.toList());
    }

    @Override
    public <T extends Transaction> void transaction(Consumer<T> runnable,
        WrapTransaction<T> wrapFunction)
    {
        try
        {
            database.transaction(rawTransaction -> {
                // Convert the raw database transaction into a Transaction instance
                final FirebaseTransaction transaction = new FirebaseTransaction(rawTransaction);

                // Wrap the transaction using the specified function
                final T wrappedTransaction = wrapFunction.wrap(transaction);

                runnable.run(wrappedTransaction);

                transaction.flush();
            });
        }
        catch (InterruptedException | ExecutionException ex)
        {
            throw new StoreException("Error applying transaction", ex);
        }
    }
}
