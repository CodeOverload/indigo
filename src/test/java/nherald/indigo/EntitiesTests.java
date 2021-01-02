package nherald.indigo;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.quality.Strictness;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

import static org.mockito.Mockito.*;

import nherald.indigo.index.Index;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.utils.TestEntity;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EntitiesTests
{
    private static final String NAMESPACE = "entities";
    private static final String INFO_ID = "info";

    private static final String WORD1 = "something";

    private static final long CURRENT_MAX_ID = 76;

    @Mock
    private Index<TestEntity> index1;

    @Mock
    private Index<TestEntity> index2;

    @Mock
    private Index<TestEntity> index3;

    @Mock
    private Store store;

    @Mock
    private TransactionWithCache transaction;

    private Entities<TestEntity> subject;

    @BeforeEach
    void before()
    {
        when(index1.getTarget()).thenReturn(entity -> "wordA wordB");
        when(index2.getTarget()).thenReturn(entity -> "wordC");
        when(index3.getTarget()).thenReturn(entity -> "wordD");

        when(index1.getId()).thenReturn("index1");
        when(index2.getId()).thenReturn("index2");
        when(index3.getId()).thenReturn("index3");

        final List<Index<TestEntity>> indices
            = List.of(index1, index2, index3);

        final EntitiesInfo info = new EntitiesInfo(CURRENT_MAX_ID);
        when(transaction.get(NAMESPACE, INFO_ID, EntitiesInfo.class)).thenReturn(info);

        subject = new Entities<>(TestEntity.class, indices, store);
    }

    @Test
    void get_looksUpCorrectIdAndType()
    {
        subject.get(34);

        verify(store).get(anyString(), eq("34"), eq(TestEntity.class));
    }

    @Test
    void getMultiple_looksUpAllIds()
    {
        final List<Long> ids = List.of(25l, 12l);

        subject.get(ids);

        final List<String> expectedIds = List.of("25", "12");

        verify(store).get(anyString(), eq(expectedIds), eq(TestEntity.class));
    }

    @Test
    void search_searchesCorrectIndex()
    {
        subject.search("index2", WORD1);

        verify(index2).get(WORD1);

        verify(index1, never()).get(anyString());
        verify(index3, never()).get(anyString());
    }

    @Test
    void search_exceptionWhenUnknownIndexSpecified()
    {
        Assertions.assertThrows(StoreException.class, () -> {
            subject.search("index_id_that_doesnt_exist", WORD1);
        });
    }

    @Test
    void put_generatesNewId_forNewEntity()
    {
        mockTransactionStart();

        final TestEntity entity = new TestEntity();

        subject.put(entity);

        Assertions.assertEquals(CURRENT_MAX_ID + 1, entity.getId());
    }

    @Test
    void put_generatesNewId_forNewEntities()
    {
        mockTransactionStart();

        // Add two entities, the second should get it's own id
        subject.put(new TestEntity());

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        Assertions.assertEquals(CURRENT_MAX_ID + 2, entity.getId());
    }

    @Test
    void put_generatesNewId_forNewEntity_storeCompletelyEmpty()
    {
        // Create a completely empty Transaction instance (crucially, this doesn't have an info object
        // stored to denote which is the next id)
        transaction = mock(TransactionWithCache.class);

        mockTransactionStart();

        final List<Index<TestEntity>> indices
            = List.of(index1, index2, index3);

        subject = new Entities<>(TestEntity.class, indices, store);

        final TestEntity entity = new TestEntity();

        subject.put(entity);

        // Should be added with an id of 1
        Assertions.assertEquals(1, entity.getId());
    }

    @Test
    void put_usesEntityId_forExistingEntity()
    {
        final Long id = 45l;

        mockTransactionStart();

        when(transaction.exists(NAMESPACE, id + "")).thenReturn(true);

        // This is an existing entity as it already has an id
        final TestEntity entity = new TestEntity();
        entity.setId(id);

        subject.put(entity);

        // The id shouldn't be changed
        Assertions.assertEquals(id, entity.getId());
    }

    @Test
    void put_addsToStore_forNewEntity()
    {
        mockTransactionStart();

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        verify(transaction).put(NAMESPACE, CURRENT_MAX_ID + 1 + "", entity);
    }

    @Test
    void put_updatesMaxId_forNewEntity()
    {
        mockTransactionStart();

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        final EntitiesInfo expectedInfo = new EntitiesInfo(CURRENT_MAX_ID + 1);

        verify(transaction).put(NAMESPACE, INFO_ID, expectedInfo);
    }

    @Test
    void put_addsToStore_forNewEntities()
    {
        mockTransactionStart();

        final TestEntity entity1 = new TestEntity();
        subject.put(entity1);

        final TestEntity entity2 = new TestEntity();
        subject.put(entity2);

        verify(transaction).put(NAMESPACE, CURRENT_MAX_ID + 1 + "", entity1);
        verify(transaction).put(NAMESPACE, CURRENT_MAX_ID + 2 + "", entity2);
    }

    @Test
    void put_addsToStore_forExistingEntity()
    {
        mockTransactionStart();

        final long id = 65;

        final TestEntity entity = new TestEntity();
        entity.setId(id);
        subject.put(entity);

        verify(transaction).put(NAMESPACE, id + "", entity);
    }

    @Test
    void put_doesntIncreaseMaxId_forExistingEntity()
    {
        mockTransactionStart();

        final long id = 65;

        final TestEntity entity = new TestEntity();
        entity.setId(id);
        subject.put(entity);

        // At the moment, info doc is always re-saved, even if the id hasn't
        // changed. This can easily be improved, but at the moment use it to
        // verify the max id doesn't change
        final EntitiesInfo expectedInfo = new EntitiesInfo(CURRENT_MAX_ID);
        verify(transaction).put(NAMESPACE, INFO_ID, expectedInfo);
    }

    @Test
    void put_addsToAllIndices_forNewEntity()
    {
        mockTransactionStart();

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        final Long id = entity.getId();

        final List<String> words1 = List.of("wordA", "wordB");
        final List<String> words2 = List.of("wordC");
        final List<String> words3 = List.of("wordD");

        verify(index1).add(eq(words1), eq(id), any());
        verify(index2).add(eq(words2), eq(id), any());
        verify(index3).add(eq(words3), eq(id), any());
    }

    @Test
    void put_addsToAllIndices_forExistingEntity()
    {
        mockTransactionStart();

        final long id = 65;

        final TestEntity entity = new TestEntity();
        entity.setId(id);
        subject.put(entity);

        final List<String> words1 = List.of("wordA", "wordB");
        final List<String> words2 = List.of("wordC");
        final List<String> words3 = List.of("wordD");

        verify(index1).add(eq(words1), eq(id), any());
        verify(index2).add(eq(words2), eq(id), any());
        verify(index3).add(eq(words3), eq(id), any());
    }

    @Test
    void put_removesOldFromAllIndices_forExistingEntity()
    {
        final Long id = 45l;

        mockTransactionStart();

        when(transaction.exists(NAMESPACE, id + "")).thenReturn(true);

        // This is an existing entity as it already has an id
        final TestEntity entity = new TestEntity();
        entity.setId(id);

        subject.put(entity);

        // Old entries should be removed
        verify(index1).remove(eq(id), any());
        verify(index2).remove(eq(id), any());
        verify(index3).remove(eq(id), any());
    }

    @Test
    void put_multiple_commitsAllChangesInOneBatch()
    {
        mockTransactionStart();

        // Put two entities at the same time and check they're all committed as part of the same batch.
        // Will involve various deletes and re-adds
        TestEntity entity1 = new TestEntity();
        entity1.setId(45l);

        TestEntity entity2 = new TestEntity();
        entity2.setId(12l);

        subject.put(List.of(entity1, entity2));

        // Only one transaction requested
        verify(store).transaction(any(), any());
    }

    @Test
    void put_multiple_updatesMaxId_forNewEntities()
    {
        mockTransactionStart();

        final TestEntity entity1 = new TestEntity();
        final TestEntity entity2 = new TestEntity();
        subject.put(List.of(entity1, entity2));

        final EntitiesInfo expectedInfo = new EntitiesInfo(CURRENT_MAX_ID + 2);

        verify(transaction).put(NAMESPACE, INFO_ID, expectedInfo);
    }

    @Test
    void put_multiple_doesntIncreaseMaxId_forExistingEntities()
    {
        mockTransactionStart();

        final long id1 = 65;
        final long id2 = 14;

        final TestEntity entity1 = new TestEntity();
        final TestEntity entity2 = new TestEntity();

        entity1.setId(id1);
        entity2.setId(id2);

        subject.put(List.of(entity1, entity2));

        // At the moment, info doc is always re-saved, even if the id hasn't
        // changed. This can easily be improved, but at the moment use it to
        // verify the max id doesn't change
        final EntitiesInfo expectedInfo = new EntitiesInfo(CURRENT_MAX_ID);
        verify(transaction).put(NAMESPACE, INFO_ID, expectedInfo);
    }

    @Test
    void put_multiple_addsToAllIndices_forExistingEntity()
    {
        when(index1.getTarget()).thenReturn(entity -> entity.getId() == 65 ? "wordA wordB" : "wordX");
        when(index2.getTarget()).thenReturn(entity -> entity.getId() == 65 ? "wordC" : "wordY wordZ");
        when(index3.getTarget()).thenReturn(entity -> entity.getId() == 65 ? "wordD" : "wordN");

        mockTransactionStart();

        final long id1 = 65;
        final long id2 = 68;

        final TestEntity entity1 = new TestEntity();
        entity1.setId(id1);

        final TestEntity entity2 = new TestEntity();
        entity2.setId(id2);

        subject.put(List.of(entity1, entity2));

        // Entity 1
        {
            final List<String> words1 = List.of("wordA", "wordB");
            final List<String> words2 = List.of("wordC");
            final List<String> words3 = List.of("wordD");

            verify(index1).add(eq(words1), eq(id1), any());
            verify(index2).add(eq(words2), eq(id1), any());
            verify(index3).add(eq(words3), eq(id1), any());
        }

        // Entity 2
        {
            final List<String> words1 = List.of("wordX");
            final List<String> words2 = List.of("wordY", "wordZ");
            final List<String> words3 = List.of("wordN");

            verify(index1).add(eq(words1), eq(id2), any());
            verify(index2).add(eq(words2), eq(id2), any());
            verify(index3).add(eq(words3), eq(id2), any());
        }
    }

    @Test
    void put_multiple_removesFromAllIndicesFirst_forExistingEntity()
    {
        mockTransactionStart();

        final long id1 = 65;
        final long id2 = 68;

        final TestEntity entity1 = new TestEntity();
        entity1.setId(id1);

        final TestEntity entity2 = new TestEntity();
        entity2.setId(id2);

        subject.put(List.of(entity1, entity2));

        final InOrder order = inOrder(index1, index2, index3);

        // Entity 1
        order.verify(index1).remove(eq(id1), any());
        order.verify(index2).remove(eq(id1), any());
        order.verify(index3).remove(eq(id1), any());

        // Entity 2
        order.verify(index1).remove(eq(id2), any());
        order.verify(index2).remove(eq(id2), any());
        order.verify(index3).remove(eq(id2), any());

        // Ensure adds are done after. Don't need to be too specific here
        order.verify(index1).add(anyList(), anyLong(), any());
        order.verify(index2).add(anyList(), anyLong(), any());
        order.verify(index3).add(anyList(), anyLong(), any());
    }

    @Test
    void delete_failsIfEntityIsntInStorage()
    {
        final Long id = 45l;

        mockTransactionStart();

        // No entity of this id is in the store
        when(transaction.exists(NAMESPACE, id + "")).thenReturn(false);

        Assertions.assertThrows(StoreException.class, () -> {
            subject.delete(id);
        });
    }

    @Test
    void delete_removesFromStorage()
    {
        final Long id = 45l;

        mockTransactionStart();

        when(transaction.exists(NAMESPACE, id + "")).thenReturn(true);

        subject.delete(id);

        verify(transaction).delete(NAMESPACE, id + "");
    }

    @Test
    void delete_removesFromAllIndices()
    {
        final Long id = 45l;

        mockTransactionStart();

        when(transaction.exists(NAMESPACE, id + "")).thenReturn(true);

        subject.delete(id);

        verify(index1).remove(eq(id), any());
        verify(index2).remove(eq(id), any());
        verify(index3).remove(eq(id), any());
    }

    @Test
    void delete_commitsAllChangesInOneBatch()
    {
        mockTransactionStart();

        when(transaction.exists(anyString(), anyString())).thenReturn(true);

        // Delete an entity and check all changes (store deletion, index deletion etc)
        // are committed as part of the same batch
        subject.delete(35l);

        // Only one transaction requested
        verify(store).transaction(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void mockTransactionStart()
    {
        // When a transaction is requested, run it as the store would do
        doAnswer(invocation -> {
                final Consumer<Transaction> runnable = (Consumer<Transaction>) invocation.getArguments()[0];
                // Run it with our mock transaction
                runnable.run(transaction);
                return null;
            })
            .when(store).transaction(any(), any());
    }
}
