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
import nherald.indigo.store.TooManyWritesException;
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
    private static final Fruit melon = new Fruit("Melon");

    private static final String appleId = "apple";
    private static final String orangeId = "orange";
    private static final String melonId = "melon";

    private static final FirebaseDocumentId appleDocId = new FirebaseDocumentId(NAMESPACE, appleId);
    private static final FirebaseDocumentId orangeDocId = new FirebaseDocumentId(NAMESPACE, orangeId);
    private static final FirebaseDocumentId melonDocId = new FirebaseDocumentId(NAMESPACE, melonId);

    private static final FirebaseDocument appleDoc = new TestFirebaseDocument(true, apple);
    private static final FirebaseDocument orangeDoc = new TestFirebaseDocument(true, orange);
    private static final FirebaseDocument notExistsDoc = new TestFirebaseDocument(false, null);

    private static final int MAX_WRITES = 500;

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
    void get_afterPut_throwsStoreException()
        throws InterruptedException, ExecutionException
    {
        final String id = "orange";

        subject.put(NAMESPACE, id, orange);

        Assertions.assertThrows(StoreException.class, () -> {
            subject.get(NAMESPACE, id, Fruit.class);
        });
    }

    @Test
    void get_afterDelete_throwsStoreException()
        throws InterruptedException, ExecutionException
    {
        final String id = "orange";

        subject.delete(NAMESPACE, id);

        Assertions.assertThrows(StoreException.class, () -> {
            subject.get(NAMESPACE, id, Fruit.class);
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

    @Test
    void exists_afterPut_throwsStoreException()
        throws InterruptedException, ExecutionException
    {
        final String id = "orange";

        subject.put(NAMESPACE, id, orange);

        Assertions.assertThrows(StoreException.class, () -> {
            subject.exists(NAMESPACE, id);
        });
    }

    @Test
    void exists_afterDelete_throwsStoreException()
        throws InterruptedException, ExecutionException
    {
        final String id = "orange";

        subject.delete(NAMESPACE, id);

        Assertions.assertThrows(StoreException.class, () -> {
            subject.exists(NAMESPACE, id);
        });
    }

    @Test
    void put_batchesUpdates()
    {
        subject.put(NAMESPACE, appleId, apple);
        subject.put(NAMESPACE, orangeId, orange);

        verify(rawTransaction, never()).set(any(), any());

        // Updates shouldn't be applied until flush() is called
        subject.flush();

        verify(rawTransaction).set(appleDocId, apple);
        verify(rawTransaction).set(orangeDocId, orange);
    }

    @Test
    void put_lastUpdateTakesPrecedence()
    {
        // Update the same entity id twice
        subject.put(NAMESPACE, appleId, apple);
        subject.put(NAMESPACE, appleId, orange);

        subject.flush();

        // Only the last should be applied
        verify(rawTransaction).set(appleDocId, orange);
    }

    @Test
    void put_lastUpdateTakesPrecedence_whenOtherEntitesUpdated()
    {
        // Update another entity too
        subject.put(NAMESPACE, melonId, melon);

        // Update the same entity id twice
        subject.put(NAMESPACE, appleId, apple);
        subject.put(NAMESPACE, appleId, orange);

        subject.flush();

        // Only the last should be applied for appleId
        verify(rawTransaction).set(appleDocId, orange);

        // The update to the other object should still happen
        verify(rawTransaction).set(melonDocId, melon);
    }

    @Test
    void put_lastUpdateTakesPrecedence_whenLastUpdateWasDelete()
    {
        // Delete the entity and then re-add
        subject.delete(NAMESPACE, appleId);
        subject.put(NAMESPACE, appleId, apple);

        subject.flush();

        // Only the last should be applied
        verify(rawTransaction).set(appleDocId, apple);
        verify(rawTransaction, never()).delete(appleDocId);
    }

    @Test
    void put_exceedsMaximumNumberOfWrites()
    {
        // Make x updates, where x is the max permitted number of writes
        for (int i = 0; i < MAX_WRITES; ++i)
        {
            final String id = "fruit" + i;
            final Fruit fruit = new Fruit(id);
            subject.put(NAMESPACE, id, fruit);
        }

        // Try to update one more - should fail with an exception
        Assertions.assertThrows(TooManyWritesException.class, () -> {
            subject.put(NAMESPACE, "another", apple);
        });
    }

    @Test
    void put_exceedsMaximumNumberOfWrites_includingSomeDeleteOperations()
    {
        // A mixture of deletion and update operations, up to the max permitted
        for (int i = 0; i < 200; ++i)
        {
            final String id = "fruit" + i;
            subject.delete(NAMESPACE, id);
        }

        for (int i = 200; i < MAX_WRITES; ++i)
        {
            final String id = "fruit" + i;
            final Fruit fruit = new Fruit(id);
            subject.put(NAMESPACE, id, fruit);
        }

        // Try to update one more - should fail with an exception
        Assertions.assertThrows(TooManyWritesException.class, () -> {
            subject.put(NAMESPACE, "another", apple);
        });
    }

    @Test
    void delete_batchesUpdates()
    {
        subject.delete(NAMESPACE, appleId);
        subject.delete(NAMESPACE, orangeId);

        verify(rawTransaction, never()).delete(any());

        // Updates shouldn't be applied until flush() is called
        subject.flush();

        verify(rawTransaction).delete(appleDocId);
        verify(rawTransaction).delete(orangeDocId);
    }

    @Test
    void delete_onlyDeletesOnceWhenEntityDeletedMultipleTimes()
    {
        // Delete the same entity id twice
        subject.delete(NAMESPACE, appleId);
        subject.delete(NAMESPACE, appleId);

        subject.flush();

        // Only one deletion should be applied to the database
        verify(rawTransaction).delete(appleDocId);
    }

    @Test
    void delete_exceedsMaximumNumberOfWrites()
    {
        // Make x deletions, where x is the max permitted number of writes
        for (int i = 0; i < MAX_WRITES; ++i)
        {
            final String id = "fruit" + i;
            subject.delete(NAMESPACE, id);
        }

        // Try to delete one more - should fail with an exception
        Assertions.assertThrows(TooManyWritesException.class, () -> {
            subject.delete(NAMESPACE, "another");
        });
    }

    @Test
    void delete_exceedsMaximumNumberOfWrites_includingSomePutOperations()
    {
        // A mixture of deletion and update operations, up to the max permitted
        for (int i = 0; i < 200; ++i)
        {
            final String id = "fruit" + i;
            subject.delete(NAMESPACE, id);
        }

        for (int i = 200; i < MAX_WRITES; ++i)
        {
            final String id = "fruit" + i;
            final Fruit fruit = new Fruit(id);
            subject.put(NAMESPACE, id, fruit);
        }

        // Try to delete one more - should fail with an exception
        Assertions.assertThrows(TooManyWritesException.class, () -> {
            subject.delete(NAMESPACE, "another");
        });
    }
}
