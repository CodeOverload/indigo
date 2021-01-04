package nherald.indigo.index;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import java.util.List;

import nherald.indigo.store.StoreException;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.utils.TestEntity;

@ExtendWith(MockitoExtension.class)
public class IndicesManagerTests
{
    @Mock
    private Index<TestEntity> index1;

    @Mock
    private Index<TestEntity> index2;

    @Mock
    private Index<TestEntity> index3;

    @Mock
    private Transaction transaction;

    private IndicesManager<TestEntity> subject;

    @BeforeEach
    void before()
    {
        final List<Index<TestEntity>> indices
            = List.of(index1, index2, index3);

        subject = new IndicesManager<>(indices);
    }

    @Test
    void search_searchesCorrectIndex()
    {
        // Set lenient on these two as the logic is likely to short-cut (once
        // it finds the one it wants it'll not bother to check the rest)
        lenient().when(index1.getId()).thenReturn("index1");
        lenient().when(index3.getId()).thenReturn("index3");

        when(index2.getId()).thenReturn("index2");

        final String word = "platypus";

        subject.search("index2", word);

        verify(index2).get(word);

        verify(index1, never()).get(anyString());
        verify(index3, never()).get(anyString());
    }

    @Test
    void search_throwsException_whenUnknownIndexSpecified()
    {
        when(index1.getId()).thenReturn("index1");
        when(index2.getId()).thenReturn("index2");
        when(index3.getId()).thenReturn("index3");

        Assertions.assertThrows(StoreException.class, () -> {
            subject.search("index_that_doesnt_exist", "platypus");
        });
    }

    @Test
    void addEntity_addsToAllIndices()
    {
        when(index1.getTarget()).thenReturn(entity -> "wordA wordB");
        when(index2.getTarget()).thenReturn(entity -> "wordC");
        when(index3.getTarget()).thenReturn(entity -> "wordD");

        final long id = 45;
        final TestEntity entity = new TestEntity();
        entity.setId(id);

        subject.addEntity(entity, transaction);

        final List<String> words1 = List.of("wordA", "wordB");
        final List<String> words2 = List.of("wordC");
        final List<String> words3 = List.of("wordD");

        verify(index1).add(eq(words1), eq(id), any());
        verify(index2).add(eq(words2), eq(id), any());
        verify(index3).add(eq(words3), eq(id), any());
    }

    @Test
    void removeEntity_removesFromAllIndices()
    {
        final long id = 45;

        subject.removeEntity(id, transaction);

        verify(index1).remove(eq(id), any());
        verify(index2).remove(eq(id), any());
        verify(index3).remove(eq(id), any());
    }
}
