package nherald.indigo;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import nherald.indigo.index.IndicesManager;
import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.utils.TestEntity;

@ExtendWith(MockitoExtension.class)
class EntitiesAdminTests
{
    @Mock
    private Entities<TestEntity> entities;

    @Mock
    private IndicesManager<TestEntity> indicesManager;

    @Mock
    private Transaction transaction;

    @InjectMocks
    private EntitiesAdmin<TestEntity> subject;

    @Test
    void regenIndices_processesAllEntities_whenNumberOfEntitiesLessThanBatchSize()
    {
        mockTransactionStart();

        final List<Long> ids = List.of(4l, 7l, 2l, 8l, 9l);

        // Number is equal to the batch size
        when(entities.list()).thenReturn(ids);

        final List<TestEntity> testEntities = ids.stream()
            .map(TestEntity::new)
            .collect(Collectors.toList());

        when(entities.get(ids)).thenReturn(testEntities);

        subject.regenIndices(5);

        ids.stream()
            .map(TestEntity::new)
            .forEach(entity -> verify(indicesManager).addEntity(entity, transaction));
    }

    @Test
    void regenIndices_processesAllEntities_whenNumberOfEntitiesGreaterThanBatchSize()
    {
        mockTransactionStart();

        final List<Long> ids = List.of(4l, 7l, 2l, 8l, 9l, 10l);

        // Number is 1 greater than the batch size
        when(entities.list()).thenReturn(ids);

        final List<TestEntity> batch1 = ids.stream()
            .limit(5)
            .map(TestEntity::new)
            .collect(Collectors.toList());

        when(entities.get(List.of(4l, 7l, 2l, 8l, 9l))).thenReturn(batch1);

        final List<TestEntity> batch2 = List.of(new TestEntity(10l));

        when(entities.get(List.of(10l))).thenReturn(batch2);

        subject.regenIndices(5);

        ids.stream()
            .map(TestEntity::new)
            .forEach(entity -> verify(indicesManager).addEntity(entity, transaction));
    }

    @Test
    void regenIndices_runsAllInOneTransaction_whenNumberOfEntitiesLessThanBatchSize()
    {
        // Number is equal to the batch size
        when(entities.list()).thenReturn(List.of(4l, 7l, 2l, 8l, 9l));

        subject.regenIndices(5);

        verify(entities).runTransaction(any());
    }

    @Test
    void regenIndices_runsInMultipleTransactions_whenNumberOfEntitiesGreaterThanBatchSize()
    {
        // One more than the batch size
        when(entities.list()).thenReturn(List.of(4l, 7l, 2l, 8l, 9l, 6l));

        subject.regenIndices(5);

        verify(entities, times(2)).runTransaction(any());
    }

    @Test
    void regenIndices_runsInMultipleTransactions_whenNumberOfEntitiesEvenGreaterThanBatchSize()
    {
        // Over twice the batch size
        when(entities.list()).thenReturn(List.of(4l, 7l, 2l, 8l, 9l, 6l, 23l, 53l, 33l, 67l, 10l));

        subject.regenIndices(5);

        verify(entities, times(3)).runTransaction(any());
    }

    @SuppressWarnings("unchecked")
    private void mockTransactionStart()
    {
        // When a transaction is requested, run it as entities would do
        doAnswer(invocation -> {
                final Consumer<Transaction> runnable = (Consumer<Transaction>) invocation.getArguments()[0];
                // Run it with our mock transaction
                runnable.run(transaction);
                return null;
            })
            .when(entities).runTransaction(any());
    }
}
