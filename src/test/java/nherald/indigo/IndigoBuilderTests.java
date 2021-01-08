package nherald.indigo;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import nherald.indigo.index.Index;
import nherald.indigo.index.IndexTestUtils;
import nherald.indigo.index.IndicesManager;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.utils.TestEntity;

@ExtendWith(MockitoExtension.class)
class IndigoBuilderTests
{
    @Mock
    private Store store;

    @Mock
    private Index<TestEntity> index1;

    @Mock
    private Index<TestEntity> index2;

    @Test
    void build_setsEntityTypeCorrectly()
    {
        final IndigoBuilder<TestEntity> subject = new IndigoBuilder<>(TestEntity.class)
            .store(store);

        final Indigo<TestEntity> actual = subject.build();

        Assertions.assertEquals(TestEntity.class, actual.getEntityType());
    }

    @Test
    void build_setsStoreCorrectly()
    {
        final IndigoBuilder<TestEntity> subject = new IndigoBuilder<>(TestEntity.class)
            .store(store);

        final Indigo<TestEntity> actual = subject.build();

        Assertions.assertEquals(store, actual.getStore());
    }

    @Test
    void build_addsSingleIndexCorrectly()
    {
        final IndigoBuilder<TestEntity> subject = new IndigoBuilder<>(TestEntity.class)
            .store(store)
            .addIndex(index1);

        final Indigo<TestEntity> actual = subject.build();

        final IndicesManager<TestEntity> manager = actual.getIndicesManager();
        final Collection<Index<TestEntity>> indices = IndexTestUtils.getIndices(manager);

        final List<Index<TestEntity>> expected = List.of(index1);

        Assertions.assertEquals(expected, indices);
    }

    @Test
    void build_addsMultipleIndicesCorrectly()
    {
        final IndigoBuilder<TestEntity> subject = new IndigoBuilder<>(TestEntity.class)
            .store(store)
            .addIndex(index1)
            .addIndex(index2);

        final Indigo<TestEntity> actual = subject.build();

        final IndicesManager<TestEntity> manager = actual.getIndicesManager();
        final Collection<Index<TestEntity>> indices = IndexTestUtils.getIndices(manager);

        final List<Index<TestEntity>> expected = List.of(index1, index2);

        Assertions.assertEquals(expected, indices);
    }

    @Test
    void build_throwsException_whenStoreNotSet()
    {
        final IndigoBuilder<TestEntity> subject = new IndigoBuilder<>(TestEntity.class);

        Assertions.assertThrows(StoreException.class, () -> subject.build());
    }
}
