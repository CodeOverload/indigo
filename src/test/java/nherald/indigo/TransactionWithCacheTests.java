package nherald.indigo;

import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import nherald.indigo.store.uow.Transaction;
import nherald.indigo.utils.Fruit;

@ExtendWith(MockitoExtension.class)
class TransactionWithCacheTests
{
    private static final String NAMESPACE1 = "namespace1";
    private static final String NAMESPACE2 = "namespace2";

    private static final Fruit apple = new Fruit("Apple");
    private static final Fruit orange = new Fruit("Orange");
    private static final Fruit banana = new Fruit("Banana");
    private static final Fruit melon = new Fruit("Melon");

    @Mock
    private Transaction transaction;

    @InjectMocks
    private TransactionWithCache subject;

    @Test
    void get_returnsEntity_whenNotCached()
    {
        final String id = "a";

        when(transaction.get(NAMESPACE1, id, Fruit.class)).thenReturn(apple);

        final Fruit actual = subject.get(NAMESPACE1, id, Fruit.class);

        Assertions.assertEquals(apple, actual);
    }

    @Test
    void get_returnsEntity_whenCached()
    {
        final String id = "a";

        when(transaction.get(NAMESPACE1, id, Fruit.class)).thenReturn(apple);

        // Call multiple times
        subject.get(NAMESPACE1, id, Fruit.class);

        final Fruit actual = subject.get(NAMESPACE1, id, Fruit.class);

        // Should return the correct entity the second time
        Assertions.assertEquals(apple, actual);
    }

    @Test
    void get_returnsEntity_whenCachedFromPut()
    {
        final String id = "a";

        // Put an object
        subject.put(NAMESPACE1, id, apple);

        // The same object should be returned by the get method
        final Fruit actual = subject.get(NAMESPACE1, id, Fruit.class);

        Assertions.assertEquals(apple, actual);
    }

    @Test
    void get_distinguishesDifferentNamespaces()
    {
        final String id = "a";

        // Put an object with the same id, but in different namespaces
        subject.put(NAMESPACE1, id, orange);
        subject.put(NAMESPACE2, id, apple);

        // Check the correct objects are returned for each namespace
        Fruit actual = subject.get(NAMESPACE1, id, Fruit.class);
        Assertions.assertEquals(orange, actual);

        actual = subject.get(NAMESPACE2, id, Fruit.class);
        Assertions.assertEquals(apple, actual);
    }

    @Test
    void get_loadsEntityOnce()
    {
        final String id = "a";

        when(transaction.get(NAMESPACE1, id, Fruit.class)).thenReturn(apple);

        // Call multiple times
        subject.get(NAMESPACE1, id, Fruit.class);
        subject.get(NAMESPACE1, id, Fruit.class);

        // It should only load from the underlying transaction once
        verify(transaction).get(NAMESPACE1, id, Fruit.class);
    }

    @Test
    void get_multiple_returnsEntities_whenNoneCached()
    {
        final String id1 = "a";
        final String id2 = "b";

        when(transaction.get(NAMESPACE1, List.of(id1, id2), Fruit.class))
            .thenReturn(List.of(apple, orange));

        final List<Fruit> actual = subject.get(NAMESPACE1, List.of(id1, id2), Fruit.class);

        final List<Fruit> expected = List.of(apple, orange);

        Assertions.assertEquals(expected, actual);

        // Should only be one look-up
        verify(transaction).get(any(), anyList(), any());
    }

    @Test
    void get_multiple_returnsEntities_whenAllCached()
    {
        final String id1 = "a";
        final String id2 = "b";

        // Put both in so they get cached
        subject.put(NAMESPACE1, id1, orange);
        subject.put(NAMESPACE1, id2, apple);


        final List<Fruit> actual = subject.get(NAMESPACE1, List.of(id1, id2), Fruit.class);

        final List<Fruit> expected = List.of(orange, apple);

        Assertions.assertEquals(expected, actual);

        // Should be no look-ups - all should come from the cache
        verify(transaction, never()).get(any(), anyList(), any());
    }

    @Test
    void get_multiple_returnsEntities_whenSomeCached()
    {
        final String id1 = "a";
        final String id2 = "b";
        final String id3 = "z";
        final String id4 = "d";

        // Put two of them so they get cached
        subject.put(NAMESPACE1, id1, apple);
        subject.put(NAMESPACE1, id3, banana);

        // Expect the other two to be fetched, so mock that fetch
        when(transaction.get(NAMESPACE1, List.of(id2, id4), Fruit.class))
            .thenReturn(List.of(orange, melon));

        final List<Fruit> actual = subject.get(NAMESPACE1, List.of(id1, id2, id3, id4), Fruit.class);

        // They must be in the same order they were requested
        final List<Fruit> expected = List.of(apple, orange, banana, melon);

        Assertions.assertEquals(expected, actual);

        // Should only be one look-up, and only for the ids that weren't in the cache
        verify(transaction).get(eq(NAMESPACE1), anyList(), eq(Fruit.class));
    }

    @Test
    void get_multiple_cachesEntities_whenNotCached()
    {
        final String id1 = "a";
        final String id2 = "b";

        when(transaction.get(NAMESPACE1, List.of(id1, id2), Fruit.class))
            .thenReturn(List.of(apple, orange));

        // This should fetch and cache the two entities
        subject.get(NAMESPACE1, List.of(id1, id2), Fruit.class);

        // Remove the mock so it can't fetch them again from storage
        reset(transaction);

        // Try again, should come from the cache
        final List<Fruit> actual = subject.get(NAMESPACE1, List.of(id1, id2), Fruit.class);

        final List<Fruit> expected = List.of(apple, orange);

        Assertions.assertEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void exists_returnsCorrectValue_whenNotCached(boolean input)
    {
        final String id = "a";

        when(transaction.exists(NAMESPACE1, id)).thenReturn(input);

        final boolean actual = subject.exists(NAMESPACE1, id);

        Assertions.assertEquals(input, actual);
    }

    @Test
    void exists_returnsTrue_whenCachedFromGet()
    {
        final String id = "a";

        when(transaction.get(NAMESPACE1, id, Fruit.class)).thenReturn(apple);

        // Get the object; it should be cached after this
        subject.get(NAMESPACE1, id, Fruit.class);

        // Exists should regard this as existing
        final boolean actual = subject.exists(NAMESPACE1, id);

        Assertions.assertTrue(actual);

        // Make sure it used the cache
        verify(transaction, never()).exists(any(), any());
    }

    @Test
    void exists_returnsFalse_whenCachedFromGet_andEntityDidntExist()
    {
        final String id = "a";

        // Entity doesn't exist
        when(transaction.get(NAMESPACE1, id, Fruit.class)).thenReturn(null);

        // Get the object; subject should cache that it doesn't exist
        subject.get(NAMESPACE1, id, Fruit.class);

        // Exists should regard this as not existing
        final boolean actual = subject.exists(NAMESPACE1, id);

        Assertions.assertFalse(actual);

        // Make sure it used the cache
        verify(transaction, never()).exists(any(), any());
    }

    @Test
    void exists_returnsTrue_whenCachedFromPut()
    {
        final String id = "a";

        // Put an object
        subject.put(NAMESPACE1, id, apple);

        // Exists should regard this as existing
        final boolean actual = subject.exists(NAMESPACE1, id);

        Assertions.assertTrue(actual);
    }

    @Test
    void exists_distinguishesDifferentNamespaces()
    {
        final String id = "a";

        // Put an object with the same id, but in another namespace
        subject.put(NAMESPACE2, id, orange);

        // Exists should not regard this as existing, as it's in a different namespace
        final boolean actual = subject.exists(NAMESPACE1, id);

        Assertions.assertFalse(actual);
    }

    @Test
    void put_storesEntity()
    {
        final String id = "a";

        subject.put(NAMESPACE1, id, apple);

        verify(transaction).put(NAMESPACE1, id, apple);
    }

    @Test
    void put_storesEntity_onSuccessiveRequests()
    {
        final String id = "a";

        // Call multiple times for the same namespace/id
        subject.put(NAMESPACE1, id, apple);
        subject.put(NAMESPACE1, id, orange);

        final InOrder order = inOrder(transaction);

        order.verify(transaction).put(NAMESPACE1, id, apple);
        order.verify(transaction).put(NAMESPACE1, id, orange);
    }

    @Test
    void delete_deletesEntity()
    {
        final String id = "a";

        subject.delete(NAMESPACE1, id);

        verify(transaction).delete(NAMESPACE1, id);
    }

    @Test
    void delete_deletesCachedEntity()
    {
        final String id = "a";

        // Add an object to cache
        subject.put(NAMESPACE1, id, orange);

        // Call delete - should remove the cached entry
        subject.delete(NAMESPACE1, id);

        // Mock it up to return a different object, so can be sure it's not using the cache
        when(transaction.get(NAMESPACE1, id, Fruit.class)).thenReturn(apple);

        // Call get() and check it's the object returned by the mock rather than the original
        final Fruit actual = subject.get(NAMESPACE1, id, Fruit.class);

        Assertions.assertEquals(apple, actual);
    }

    @Test
    void delete_deletesWhenCached()
    {
        final String id = "a";

        // Add an object to cache
        subject.put(NAMESPACE1, id, orange);

        subject.delete(NAMESPACE1, id);

        verify(transaction).delete(NAMESPACE1, id);
    }
}
