package nherald.indigo.store.firebase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
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
    private static final Fruit orange = new Fruit("Orange");

    private static final FirebaseDocument appleDoc = new TestFirebaseDocument(true, apple);
    private static final FirebaseDocument orangeDoc = new TestFirebaseDocument(true, orange);
    private static final FirebaseDocument notExistsDoc = new TestFirebaseDocument(false, null);

    @Mock
    private FirebaseRawTransaction rawTransaction;

    @InjectMocks
    private FirebaseTransaction subject;

    @Test
    void get_returnsNull_whenNotExists()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.getAll(any())).thenReturn(List.of(notExistsDoc));

        final Fruit actual = subject.get(NAMESPACE, "45", Fruit.class);

        Assertions.assertNull(actual);
    }

    @Test
    void get_returnsEntity_whenExists()
        throws InterruptedException, ExecutionException
    {
        final String id = "apple";
        final FirebaseDocumentId docId = new FirebaseDocumentId(NAMESPACE, id);

        when(rawTransaction.getAll(List.of(docId)))
            .thenReturn(List.of(appleDoc));

        final Fruit actual = subject.get(NAMESPACE, id, Fruit.class);

        Assertions.assertEquals(apple, actual);
    }

    @Test
    void get_throwsStoreException_onInterruptedException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.getAll(any()))
            .thenThrow(new InterruptedException());

        Assertions.assertThrows(StoreException.class, () -> {
            subject.get(NAMESPACE, "orange", Fruit.class);
        });
    }

    @Test
    void get_throwsStoreException_onExecutionException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.getAll(any()))
            .thenThrow(new ExecutionException(new Throwable()));

        Assertions.assertThrows(StoreException.class, () -> {
            subject.get(NAMESPACE, "orange", Fruit.class);
        });
    }

    @Test
    void get_multiple_returnsNull_whenNotExists()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.getAll(any()))
            .thenReturn(List.of(appleDoc, notExistsDoc));

        final List<Fruit> actual = subject.get(NAMESPACE, List.of("46", "12"), Fruit.class);

        final List<Fruit> expected = Arrays.asList(apple, null);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void get_multiple_returnsEntity_whenExists()
        throws InterruptedException, ExecutionException
    {
        final String id1 = "apple";
        final String id2 = "orange";

        final FirebaseDocumentId docId1 = new FirebaseDocumentId(NAMESPACE, id1);
        final FirebaseDocumentId docId2 = new FirebaseDocumentId(NAMESPACE, id2);

        when(rawTransaction.getAll(List.of(docId2, docId1)))
            .thenReturn(List.of(orangeDoc, appleDoc));

        final List<Fruit> actual = subject.get(NAMESPACE, List.of(id2, id1), Fruit.class);

        Assertions.assertEquals(List.of(orange, apple), actual);
    }

    @Test
    void get_multiple_throwsStoreException_onInterruptedException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.getAll(any()))
            .thenThrow(new InterruptedException());

        final List<String> ids = List.of("orange");

        Assertions.assertThrows(StoreException.class, () -> {
            subject.get(NAMESPACE, ids, Fruit.class);
        });
    }

    @Test
    void get_multiple_throwsStoreException_onExecutionException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.getAll(any()))
            .thenThrow(new ExecutionException(new Throwable()));

        final List<String> ids = List.of("orange");

        Assertions.assertThrows(StoreException.class, () -> {
            subject.get(NAMESPACE, ids, Fruit.class);
        });
    }

    @Test
    void exists_returnsFalse_whenNotExists()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.get(any()))
            .thenReturn(notExistsDoc);

        final boolean actual = subject.exists(NAMESPACE, "45");

        Assertions.assertFalse(actual);
    }

    @Test
    void exists_returnsTrue_whenExists()
        throws InterruptedException, ExecutionException
    {
        final String id = "apple";
        final FirebaseDocumentId docId = new FirebaseDocumentId(NAMESPACE, id);

        when(rawTransaction.get(docId))
            .thenReturn(appleDoc);

        final boolean actual = subject.exists(NAMESPACE, id);

        Assertions.assertTrue(actual);
    }

    @Test
    void exists_throwsStoreException_onInterruptedException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.get(any()))
            .thenThrow(new InterruptedException());

        Assertions.assertThrows(StoreException.class, () -> {
            subject.exists(NAMESPACE, "orange");
        });
    }

    @Test
    void exists_throwsStoreException_onExecutionException()
        throws InterruptedException, ExecutionException
    {
        when(rawTransaction.get(any()))
            .thenThrow(new ExecutionException(new Throwable()));

        Assertions.assertThrows(StoreException.class, () -> {
            subject.exists(NAMESPACE, "orange");
        });
    }
}
