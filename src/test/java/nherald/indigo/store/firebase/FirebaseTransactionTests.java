package nherald.indigo.store.firebase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import java.util.concurrent.ExecutionException;

import nherald.indigo.store.StoreException;
import nherald.indigo.store.firebase.db.FirebaseDocument;
import nherald.indigo.store.firebase.db.FirebaseDocumentId;
import nherald.indigo.store.firebase.db.FirebaseRawTransaction;
import nherald.indigo.utils.Fruit;

@ExtendWith(MockitoExtension.class)
class FirebaseTransactionTests
{
    private static final String NAMESPACE = "transaction-tests";

    private static final Fruit apple = new Fruit("Apple");

    private static final FirebaseDocument appleDocument = new TestFirebaseDocument(true, apple);
    private static final FirebaseDocument notExistsDoc = new TestFirebaseDocument(false, null);

    @Mock
    private FirebaseRawTransaction rawTransaction;

    @InjectMocks
    private FirebaseTransaction subject;

    @Test
    void get_returnsNull_whenNotExists()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.get(any())).thenReturn(notExistsDoc);

        final Fruit actual = subject.get(NAMESPACE, "45", Fruit.class);

        Assertions.assertNull(actual);
    }

    @Test
    void get_returnsEntity_whenExists()
        throws InterruptedException, ExecutionException
    {
        final String id = "apple";
        final FirebaseDocumentId docId = new FirebaseDocumentId(NAMESPACE, id);

        when(rawTransaction.get(docId)).thenReturn(appleDocument);

        final Fruit actual = subject.get(NAMESPACE, id, Fruit.class);

        Assertions.assertEquals(apple, actual);
    }

    @Test
    void get_throwsStoreException_onInterruptedException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.get(any())).thenThrow(new InterruptedException());

        Assertions.assertThrows(StoreException.class, () -> {
            subject.get(NAMESPACE, "orange", Fruit.class);
        });
    }

    @Test
    void get_throwsStoreException_onExecutionException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.get(any())).thenThrow(new ExecutionException(new Throwable()));

        Assertions.assertThrows(StoreException.class, () -> {
            subject.get(NAMESPACE, "orange", Fruit.class);
        });
    }

    @Test
    void exists_returnsFalse_whenNotExists()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.get(any())).thenReturn(notExistsDoc);

        final boolean actual = subject.exists(NAMESPACE, "45");

        Assertions.assertFalse(actual);
    }

    @Test
    void exists_returnsTrue_whenExists()
        throws InterruptedException, ExecutionException
    {
        final String id = "apple";
        final FirebaseDocumentId docId = new FirebaseDocumentId(NAMESPACE, id);

        when(rawTransaction.get(docId)).thenReturn(appleDocument);

        final boolean actual = subject.exists(NAMESPACE, id);

        Assertions.assertTrue(actual);
    }

    @Test
    void exists_throwsStoreException_onInterruptedException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.get(any())).thenThrow(new InterruptedException());

        Assertions.assertThrows(StoreException.class, () -> {
            subject.exists(NAMESPACE, "orange");
        });
    }

    @Test
    void exists_throwsStoreException_onExecutionException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.get(any())).thenThrow(new ExecutionException(new Throwable()));

        Assertions.assertThrows(StoreException.class, () -> {
            subject.exists(NAMESPACE, "orange");
        });
    }
}
