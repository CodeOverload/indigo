package nherald.indigo.store.firebase;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.firebase.db.FirebaseRawDatabase;
import nherald.indigo.store.firebase.db.FirebaseRawDocumentId;
import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.store.uow.WrapTransaction;

public class FirebaseStore extends FirebaseReadOps implements Store
{
    private final FirebaseRawDatabase database;

    public FirebaseStore(FirebaseRawDatabase database)
    {
        super(database);

        this.database = database;
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
