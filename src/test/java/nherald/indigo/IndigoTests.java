package nherald.indigo;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import nherald.indigo.index.IndicesManager;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.utils.TestEntity;

@ExtendWith(MockitoExtension.class)
class IndigoTests
{
    private static final String NAMESPACE = "entities";
    private static final String INFO_ID = "info";

    private static final long CURRENT_MAX_ID = 76;

    @Mock
    private IndicesManager<TestEntity> indicesManager;

    @Mock
    private Store store;

    @Mock
    private TransactionWithCache transaction;

    private Indigo<TestEntity> subject;

    @BeforeEach
    void before()
    {
        subject = new Indigo<>(TestEntity.class, indicesManager, store);
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
    void list_listsAllIds()
    {
        when(store.list(NAMESPACE)).thenReturn(List.of("1", "4"));

        final Collection<Long> actual = subject.list();

        final Collection<Long> expected = List.of(1l, 4l);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void list_filtersOutInfo()
    {
        when(store.list(NAMESPACE)).thenReturn(List.of("45", "info", "31"));

        final Collection<Long> actual = subject.list();

        final Collection<Long> expected = List.of(45l, 31l);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void put_generatesNewId_forNewEntity()
    {
        mockStoredInfo();

        mockTransactionStart();

        final TestEntity entity = new TestEntity();

        subject.put(entity);

        Assertions.assertEquals(CURRENT_MAX_ID + 1, entity.getId());
    }

    @Test
    void put_generatesNewId_forNewEntities()
    {
        mockStoredInfo();

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
        // Deliberately don't mock an EntitiesInfo object in the store

        mockTransactionStart();

        final TestEntity entity = new TestEntity();

        subject.put(entity);

        // Should be added with an id of 1
        Assertions.assertEquals(1, entity.getId());
    }

    @Test
    void put_usesEntityId_forExistingEntity()
    {
        mockStoredInfo();

        final Long id = 45l;

        mockTransactionStart();

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
        mockStoredInfo();

        mockTransactionStart();

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        verify(transaction).put(NAMESPACE, CURRENT_MAX_ID + 1 + "", entity);
    }

    @Test
    void put_updatesMaxId_forNewEntity()
    {
        mockStoredInfo();

        mockTransactionStart();

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        final EntitiesInfo expectedInfo = new EntitiesInfo(CURRENT_MAX_ID + 1);

        verify(transaction).put(NAMESPACE, INFO_ID, expectedInfo);
    }

    @Test
    void put_addsToStore_forNewEntities()
    {
        mockStoredInfo();

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
        mockStoredInfo();

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
        mockStoredInfo();

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
        mockStoredInfo();

        mockTransactionStart();

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        verify(indicesManager).addEntity(eq(entity), any());
    }

    @Test
    void put_addsToAllIndices_forExistingEntity()
    {
        mockStoredInfo();

        mockTransactionStart();

        final long id = 65;

        final TestEntity entity = new TestEntity();
        entity.setId(id);

        subject.put(entity);

        verify(indicesManager).addEntity(eq(entity), any());
    }

    @Test
    void put_removesOldFromAllIndices_forExistingEntity()
    {
        mockStoredInfo();

        final Long id = 45l;

        mockTransactionStart();

        // This is an existing entity as it already has an id
        final TestEntity entity = new TestEntity();
        entity.setId(id);

        subject.put(entity);

        // Old entries should be removed
        verify(indicesManager).removeEntity(eq(id), any());
    }

    @Test
    void put_multiple_commitsAllChangesInOneBatch()
    {
        mockStoredInfo();

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
        mockStoredInfo();

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
        mockStoredInfo();

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
        mockStoredInfo();

        mockTransactionStart();

        final long id1 = 65;
        final long id2 = 68;

        final TestEntity entity1 = new TestEntity();
        entity1.setId(id1);

        final TestEntity entity2 = new TestEntity();
        entity2.setId(id2);

        subject.put(List.of(entity1, entity2));

        verify(indicesManager).addEntity(eq(entity1), any());
        verify(indicesManager).addEntity(eq(entity2), any());
    }

    @Test
    void put_multiple_removesFromAllIndicesFirst_forExistingEntity()
    {
        mockStoredInfo();

        mockTransactionStart();

        final long id1 = 65;
        final long id2 = 68;

        final TestEntity entity1 = new TestEntity();
        entity1.setId(id1);

        final TestEntity entity2 = new TestEntity();
        entity2.setId(id2);

        subject.put(List.of(entity1, entity2));

        final InOrder order = inOrder(indicesManager);

        // Ensure adds are done after
        order.verify(indicesManager).removeEntity(eq(id1), any());
        order.verify(indicesManager).addEntity(eq(entity1), any());

        order.verify(indicesManager).removeEntity(eq(id2), any());
        order.verify(indicesManager).addEntity(eq(entity2), any());
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
    void delete_removesFromIndices()
    {
        final Long id = 45l;

        mockTransactionStart();

        when(transaction.exists(NAMESPACE, id + "")).thenReturn(true);

        subject.delete(id);

        verify(indicesManager).removeEntity(eq(id), any());
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

    /**
     * Mocks up the store such that there's an EntitiesInfo object stored with
     * CURRENT_MAX_ID as the current maxId
     */
    private void mockStoredInfo()
    {
        final EntitiesInfo info = new EntitiesInfo(CURRENT_MAX_ID);
        when(transaction.get(NAMESPACE, INFO_ID, EntitiesInfo.class))
            .thenReturn(info);
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
