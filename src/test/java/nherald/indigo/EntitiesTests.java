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
import nherald.indigo.uow.BatchUpdate;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class EntitiesTests
{
    private static final String WORD1 = "something";

    private static final long CURRENT_MAX_ID = 76;

    private static final String NAMESPACE = "entities";

    @Mock
    private Index<TestEntity> index1;

    @Mock
    private Index<TestEntity> index2;

    @Mock
    private Index<TestEntity> index3;

    @Mock
    private Store store;

    @Mock
    private BatchUpdate batch;

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
        when(store.get(NAMESPACE, "info", EntitiesInfo.class)).thenReturn(info);

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
        when(store.startBatch()).thenReturn(batch);

        final TestEntity entity = new TestEntity();

        subject.put(entity);

        Assertions.assertEquals(CURRENT_MAX_ID + 1, entity.getId());
    }

    @Test
    void put_generatesNewId_forNewEntities()
    {
        when(store.startBatch()).thenReturn(batch);

        // Add two entities, the second should get it's own id
        subject.put(new TestEntity());

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        Assertions.assertEquals(CURRENT_MAX_ID + 2, entity.getId());
    }

    @Test
    void put_generatesNewId_forNewEntity_storeCompletelyEmpty()
    {
        // Create a completely empty Store instance (crucially, this doesn't have an info object
        // stored to denote which is the next id)
        store = mock(Store.class);

        when(store.startBatch()).thenReturn(batch);

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

        when(store.startBatch()).thenReturn(batch);

        when(store.exists(NAMESPACE, id + "")).thenReturn(true);

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
        when(store.startBatch()).thenReturn(batch);

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        verify(store).put(NAMESPACE, CURRENT_MAX_ID + 1 + "", entity, batch);
    }

    @Test
    void put_addsToStore_forNewEntities()
    {
        when(store.startBatch()).thenReturn(batch);

        final TestEntity entity1 = new TestEntity();
        subject.put(entity1);

        final TestEntity entity2 = new TestEntity();
        subject.put(entity2);

        verify(store).put(NAMESPACE, CURRENT_MAX_ID + 1 + "", entity1, batch);
        verify(store).put(NAMESPACE, CURRENT_MAX_ID + 2 + "", entity2, batch);
    }

    @Test
    void put_addsToStore_forExistingEntity()
    {
        when(store.startBatch()).thenReturn(batch);

        final long id = 65;

        when(store.exists(NAMESPACE, id + "")).thenReturn(true);

        final TestEntity entity = new TestEntity();
        entity.setId(id);
        subject.put(entity);

        verify(store).put(NAMESPACE, id + "", entity, batch);
    }

    @Test
    void put_addsToAllIndices_forNewEntity()
    {
        when(store.startBatch()).thenReturn(batch);

        final TestEntity entity = new TestEntity();
        subject.put(entity);

        final Long id = entity.getId();

        final List<String> words1 = List.of("wordA", "wordB");
        final List<String> words2 = List.of("wordC");
        final List<String> words3 = List.of("wordD");

        verify(index1).add(words1, id, batch);
        verify(index2).add(words2, id, batch);
        verify(index3).add(words3, id, batch);
    }

    @Test
    void put_addsToAllIndices_forExistingEntity()
    {
        when(store.startBatch()).thenReturn(batch);

        final long id = 65;

        when(store.exists(NAMESPACE, id + "")).thenReturn(true);

        final TestEntity entity = new TestEntity();
        entity.setId(id);
        subject.put(entity);

        final List<String> words1 = List.of("wordA", "wordB");
        final List<String> words2 = List.of("wordC");
        final List<String> words3 = List.of("wordD");

        verify(index1).add(words1, id, batch);
        verify(index2).add(words2, id, batch);
        verify(index3).add(words3, id, batch);
    }

    @Test
    void put_doesntTryToDeleteOld_forNewEntity()
    {
        when(store.startBatch()).thenReturn(batch);

        final TestEntity entity = new TestEntity();

        subject.put(entity);

        verify(store, never()).delete(anyString(), anyString(), any());
    }

    @Test
    void put_deletesOld_forExistingEntity()
    {
        final Long id = 45l;

        when(store.startBatch()).thenReturn(batch);

        when(store.exists(NAMESPACE, id + "")).thenReturn(true);

        // This is an existing entity as it already has an id
        final TestEntity entity = new TestEntity();
        entity.setId(id);

        subject.put(entity);

        verify(store).delete(NAMESPACE, id + "", batch);
    }

    @Test
    void put_deletesOldFromAllIndices_forExistingEntity()
    {
        final Long id = 45l;

        when(store.startBatch()).thenReturn(batch);

        when(store.exists(NAMESPACE, id + "")).thenReturn(true);

        // This is an existing entity as it already has an id
        final TestEntity entity = new TestEntity();
        entity.setId(id);

        subject.put(entity);

        // Old entries should be removed
        verify(index1).remove(id, batch);
        verify(index2).remove(id, batch);
        verify(index3).remove(id, batch);
    }

    @Test
    void put_deletesOldThenReAddToStore_forExistingEntity()
    {
        final Long id = 45l;

        when(store.startBatch()).thenReturn(batch);

        when(store.exists(NAMESPACE, id + "")).thenReturn(true);

        // This is an existing entity as it already has an id
        final TestEntity entity = new TestEntity();
        entity.setId(id);

        subject.put(entity);

        final InOrder order = inOrder(store);

        // Earlier tests just check delete & put are called independently. Check
        // they're done in the correct order here
        order.verify(store).delete(NAMESPACE, id + "", batch);
        order.verify(store).put(NAMESPACE, id + "", entity, batch);
    }

    @Test
    void put_deletesOldThenReAddToIndices_forExistingEntity()
    {
        final Long id = 45l;

        when(store.startBatch()).thenReturn(batch);

        when(store.exists(NAMESPACE, id + "")).thenReturn(true);

        // This is an existing entity as it already has an id
        final TestEntity entity = new TestEntity();
        entity.setId(id);

        subject.put(entity);

        final InOrder order = inOrder(index1);

        final List<String> words1 = List.of("wordA", "wordB");

        // Earlier tests just check delete & put are called independently. Check
        // they're done in the correct order here. Note; just checking one of the
        // indices here - no need to check the others
        order.verify(index1).remove(id, batch);
        order.verify(index1).add(words1, id, batch);
    }

    @Test
    void put_commitsAllChangesInOneBatch()
    {
        when(store.startBatch()).thenReturn(batch);

        when(store.exists(anyString(), anyString())).thenReturn(true);

        // Put two entities at the same time and check they're all committed as part of the same batch.
        // Will involve various deletes and re-adds
        TestEntity entity1 = new TestEntity();
        entity1.setId(45l);

        TestEntity entity2 = new TestEntity();
        entity2.setId(12l);

        subject.put(List.of(entity1, entity2));

        final InOrder order = inOrder(store, index1, index2, index3, batch);

        // Check no more interactions after the commit
        order.verify(batch).commit();
        order.verifyNoMoreInteractions();
    }

    @Test
    void put_failsIfEntityIsntInStorage_forExistingEntity()
    {
        final Long id = 45l;

        when(store.startBatch()).thenReturn(batch);

        // No entity of this id is in the store
        when(store.exists(NAMESPACE, id + "")).thenReturn(false);

        // This is (supposed to be) an existing entity as it already has an id
        final TestEntity entity = new TestEntity();
        entity.setId(id);

        Assertions.assertThrows(StoreException.class, () -> {
            subject.put(entity);
        });
    }

    @Test
    void delete_failsIfEntityIsntInStorage()
    {
        final Long id = 45l;

        when(store.startBatch()).thenReturn(batch);

        // No entity of this id is in the store
        when(store.exists(NAMESPACE, id + "")).thenReturn(false);

        Assertions.assertThrows(StoreException.class, () -> {
            subject.delete(id);
        });
    }

    @Test
    void delete_removesFromStorage()
    {
        final Long id = 45l;

        when(store.startBatch()).thenReturn(batch);

        // No entity of this id is in the store
        when(store.exists(NAMESPACE, id + "")).thenReturn(true);

        subject.delete(id);

        verify(store).delete(NAMESPACE, id + "", batch);
    }

    @Test
    void delete_removesFromAllIndices()
    {
        final Long id = 45l;

        when(store.startBatch()).thenReturn(batch);

        // No entity of this id is in the store
        when(store.exists(NAMESPACE, id + "")).thenReturn(true);

        subject.delete(id);

        verify(index1).remove(id, batch);
        verify(index2).remove(id, batch);
        verify(index3).remove(id, batch);
    }

    @Test
    void delete_commitsAllChangesInOneBatch()
    {
        when(store.startBatch()).thenReturn(batch);

        when(store.exists(anyString(), anyString())).thenReturn(true);

        // Delete an entity and check all changes (store deletion, index deletion etc)
        // are committed as part of the same batch
        subject.delete(35l);

        final InOrder order = inOrder(store, index1, index2, index3, batch);

        // Check no more interactions after the commit
        order.verify(batch).commit();
        order.verifyNoMoreInteractions();
    }
}
