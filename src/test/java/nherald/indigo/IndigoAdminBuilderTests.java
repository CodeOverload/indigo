package nherald.indigo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import nherald.indigo.index.IndicesManager;
import nherald.indigo.store.StoreException;
import nherald.indigo.utils.TestEntity;

@ExtendWith(MockitoExtension.class)
class IndigoAdminBuilderTests
{
    @Mock
    private Indigo<TestEntity> indigo;

    @Mock
    private IndicesManager<TestEntity> indicesManager;

    @Test
    void build_setsIndigoCorrectly()
    {
        final IndigoAdminBuilder<TestEntity> subject = new IndigoAdminBuilder<TestEntity>()
            .indigo(indigo);

        final IndigoAdmin<TestEntity> actual = subject.build();

        Assertions.assertEquals(indigo, actual.getIndigo());
    }

    @Test
    void build_setsIndicesManagerCorrectly()
    {
        when(indigo.getIndicesManager()).thenReturn(indicesManager);

        final IndigoAdminBuilder<TestEntity> subject = new IndigoAdminBuilder<TestEntity>()
            .indigo(indigo);

        final IndigoAdmin<TestEntity> actual = subject.build();

        Assertions.assertEquals(indicesManager, actual.getIndicesManager());
    }

    @Test
    void build_throwsException_whenIndigoNotSet()
    {
        final IndigoAdminBuilder<TestEntity> subject = new IndigoAdminBuilder<TestEntity>();

        Assertions.assertThrows(StoreException.class, () -> subject.build());
    }
}
