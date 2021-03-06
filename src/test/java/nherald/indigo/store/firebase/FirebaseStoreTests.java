package nherald.indigo.store.firebase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

import nherald.indigo.store.firebase.db.FirebaseRawDatabase;
import nherald.indigo.store.firebase.db.FirebaseRawDocument;
import nherald.indigo.store.firebase.db.FirebaseRawDocumentId;
import nherald.indigo.store.firebase.db.FirebaseRawTransaction;
import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;
import nherald.indigo.utils.Fruit;

@ExtendWith(MockitoExtension.class)
class FirebaseStoreTests
{
    private static final String NAMESPACE = "fruit";

    private static final Fruit apple = new Fruit("Apple");
    private static final Fruit pear = new Fruit("Pear");
    private static final Fruit orange = new Fruit("Orange");

    private static final FirebaseRawDocument appleDocument = new TestFirebaseDocument(true, apple);
    private static final FirebaseRawDocument pearDocument = new TestFirebaseDocument(true, pear);
    private static final FirebaseRawDocument orangeDocument = new TestFirebaseDocument(true, orange);

    private static final FirebaseRawDocument notExistDocument = new TestFirebaseDocument(false, null);

    @Mock
    private FirebaseRawDatabase database;

    @Mock
    private FirebaseRawTransaction rawTransaction;

    @Mock
    private Transaction wrappedTransaction;

    @Mock
    private Consumer<Transaction> runnable;

    private FirebaseStore subject;

    @BeforeEach
    void before()
    {
        subject = new FirebaseStore(database);
    }

    @Test
    void get_single_returnsEntity_whenExists()
        throws InterruptedException, ExecutionException
    {
        final List<FirebaseRawDocumentId> ids = createIds("orange");

        when(database.getAll(ids))
            .thenReturn(List.of(
                orangeDocument
            ));

        final Fruit actual = subject.get(NAMESPACE, "orange", Fruit.class);

        final Fruit expected = orange;

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void get_single_returnsNull_whenEntityDoesntExist()
        throws InterruptedException, ExecutionException
    {
        final List<FirebaseRawDocumentId> ids = createIds("orange");

        when(database.getAll(ids))
            .thenReturn(List.of(
                notExistDocument
            ));

        final Fruit actual = subject.get(NAMESPACE, "orange", Fruit.class);

        Assertions.assertNull(actual);
    }

    @Test
    void get_multiple_returnsAll_whenAllExist()
        throws InterruptedException, ExecutionException
    {
        final List<FirebaseRawDocumentId> ids = createIds("apple", "pear", "orange");

        when(database.getAll(ids))
            .thenReturn(List.of(
                appleDocument,
                pearDocument,
                orangeDocument
            ));

        final List<Fruit> actual = subject.get(NAMESPACE, List.of("apple", "pear", "orange"), Fruit.class);

        final List<Fruit> expected = Arrays.asList(apple, pear, orange);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void get_multiple_returnsNull_whenEntityDoesntExist()
        throws InterruptedException, ExecutionException
    {
        final List<FirebaseRawDocumentId> ids = createIds("apple", "pear", "orange");

        when(database.getAll(ids))
            .thenReturn(List.of(
                appleDocument,
                notExistDocument, // This doesn't exist, so expect 2nd result to be null
                orangeDocument
            ));

        final List<Fruit> actual = subject.get(NAMESPACE, List.of("apple", "pear", "orange"), Fruit.class);

        final List<Fruit> expected = Arrays.asList(apple, null, orange);

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void list_whenSomeEntitiesExist()
        throws InterruptedException, ExecutionException
    {
        final List<FirebaseRawDocumentId> ids = createIds("apple", "pear", "orange");

        when(database.list(NAMESPACE))
            .thenReturn(ids);

        final Collection<String> actual = subject.list(NAMESPACE);

        final Collection<String> expected = List.of("apple", "pear", "orange");

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void list_whenNoEntitiesExist()
        throws InterruptedException, ExecutionException
    {
        final List<FirebaseRawDocumentId> ids = Collections.emptyList();

        when(database.list(NAMESPACE))
            .thenReturn(ids);

        final Collection<String> actual = subject.list(NAMESPACE);

        final Collection<String> expected = Collections.emptyList();

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void transaction_wrapsInnerTransaction()
        throws InterruptedException, ExecutionException
    {
        // When a transaction is requested from the underlying database instance,
        // run the specified runnable just as the database would
        doAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                final Consumer<FirebaseRawTransaction> runnable = (Consumer<FirebaseRawTransaction>) invocation.getArguments()[0];
                runnable.run(rawTransaction);
                return null;
            })
            .when(database).transaction(any());


        subject.transaction(
            runnable,
            // In the function that wraps, just return a new transaction (that
            // represents a wrapped transaction)
            rawTransaction -> wrappedTransaction);

        // It should call run() on the wrapped transaction, not the original
        verify(runnable).run(eq(wrappedTransaction));
    }

    private static final List<FirebaseRawDocumentId> createIds(String ... ids)
    {
        return Stream.of(ids)
            .map(FirebaseStoreTests::createId)
            .collect(Collectors.toList());
    }

    private static final FirebaseRawDocumentId createId(String id)
    {
        return new FirebaseRawDocumentId(NAMESPACE, id);
    }
}

