package nherald.indigo.store.firebase;

import java.util.Arrays;
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

import nherald.indigo.store.firebase.db.FirebaseDatabase;
import nherald.indigo.store.firebase.db.FirebaseDocument;
import nherald.indigo.store.firebase.db.FirebaseDocumentId;
import nherald.indigo.utils.Fruit;

@ExtendWith(MockitoExtension.class)
public class FirebaseStoreTests
{
    private static final String NAMESPACE = "fruit";

    private static final Fruit apple = new Fruit("Apple");
    private static final Fruit pear = new Fruit("Pear");
    private static final Fruit orange = new Fruit("Orange");

    private static final FirebaseDocument appleDocument = new TestFirebaseDocument(true, apple);
    private static final FirebaseDocument pearDocument = new TestFirebaseDocument(true, pear);
    private static final FirebaseDocument orangeDocument = new TestFirebaseDocument(true, orange);

    private static final FirebaseDocument notExistDocument = new TestFirebaseDocument(false, null);

    @Mock
    private FirebaseDatabase database;

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
        final List<FirebaseDocumentId> ids = createIds("orange");

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
        final List<FirebaseDocumentId> ids = createIds("orange");

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
        final List<FirebaseDocumentId> ids = createIds("apple", "pear", "orange");

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
        final List<FirebaseDocumentId> ids = createIds("apple", "pear", "orange");

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

    private static final List<FirebaseDocumentId> createIds(String ... ids)
    {
        return Stream.of(ids)
            .map(FirebaseStoreTests::createId)
            .collect(Collectors.toList());
    }

    private static final FirebaseDocumentId createId(String id)
    {
        return new FirebaseDocumentId(NAMESPACE, id);
    }
}

