package nherald.indigo.index;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import nherald.indigo.index.terms.BasicWordFilter;
import nherald.indigo.index.terms.PrefixWordSelector;
import nherald.indigo.index.terms.WordFilter;
import nherald.indigo.index.terms.WordSelector;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.utils.TestEntity;

@ExtendWith(MockitoExtension.class)
class IndexBuilderTests
{
    @Mock
    private Store store;

    @Mock
    private IndexTarget<TestEntity> target;

    @Mock
    private WordFilter wordFilter;

    @Mock
    private WordSelector wordSelector;

    @Test
    void ctor_throwsException_whenNullId()
    {
        Assertions.assertThrows(StoreException.class, () -> new IndexBuilder<TestEntity>(null));
    }

    @Test
    void build_setsCorrectId_whenAllRequiredOptionsSet()
    {
        final String id = "description";

        final IndexBuilder<TestEntity> subject = new IndexBuilder<TestEntity>(id)
            .store(store)
            .target(target)
            .wordFilter(wordFilter)
            .wordSelector(wordSelector);

        final Index<TestEntity> actual = subject.build();

        Assertions.assertEquals(id, actual.getId());
    }

    @Test
    void build_setsCorrectTarget_whenAllRequiredOptionsSet()
    {
        final String id = "description";

        final IndexBuilder<TestEntity> subject = new IndexBuilder<TestEntity>(id)
            .store(store)
            .target(target)
            .wordFilter(wordFilter)
            .wordSelector(wordSelector);

        final Index<TestEntity> actual = subject.build();

        Assertions.assertEquals(target, actual.getTarget());
    }

    @Test
    void build_setsCorrectStore_whenAllRequiredOptionsSet()
    {
        final String id = "description";

        final IndexBuilder<TestEntity> subject = new IndexBuilder<TestEntity>(id)
            .store(store)
            .target(target)
            .wordFilter(wordFilter)
            .wordSelector(wordSelector);

        final Index<TestEntity> actual = subject.build();

        Assertions.assertEquals(store, actual.getStore());
    }

    @Test
    void build_setsCorrectWordSelector_whenAllRequiredOptionsSet()
    {
        final String id = "description";

        final IndexBuilder<TestEntity> subject = new IndexBuilder<TestEntity>(id)
            .store(store)
            .target(target)
            .wordFilter(wordFilter)
            .wordSelector(wordSelector);

        final Index<TestEntity> actual = subject.build();

        Assertions.assertEquals(wordSelector, actual.getWordSelector());
    }

    @Test
    void build_usesDefaultWordFilter_whenWordFilterNotSet()
    {
        final IndexBuilder<TestEntity> subject = new IndexBuilder<TestEntity>("description")
            .store(store)
            .target(target)
            .wordSelector(wordSelector);

        final Index<TestEntity> actual = subject.build();

        final WordFilter actualWordFilter = actual.getWordFilter();
        Assertions.assertTrue(actualWordFilter instanceof BasicWordFilter);
    }

    @Test
    void build_usesDefaultWordSelector_whenWordSelectorNotSet()
    {
        final IndexBuilder<TestEntity> subject = new IndexBuilder<TestEntity>("description")
            .store(store)
            .target(target)
            .wordFilter(wordFilter);

        final Index<TestEntity> actual = subject.build();

        final WordSelector actualWordSelector = actual.getWordSelector();
        Assertions.assertTrue(actualWordSelector instanceof PrefixWordSelector);
    }

    @Test
    void build_throwsException_whenTargetNotSet()
    {
        final IndexBuilder<TestEntity> subject = new IndexBuilder<TestEntity>("description")
            .store(store)
            .wordFilter(wordFilter)
            .wordSelector(wordSelector);

        Assertions.assertThrows(StoreException.class, () -> subject.build());
    }

    @Test
    void build_throwsException_whenStoreNotSet()
    {
        final IndexBuilder<TestEntity> subject = new IndexBuilder<TestEntity>("description")
            .target(target)
            .wordFilter(wordFilter)
            .wordSelector(wordSelector);

        Assertions.assertThrows(StoreException.class, () -> subject.build());
    }
}
