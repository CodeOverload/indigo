package nherald.indigo.store.file;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import nherald.indigo.store.IdValidator;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.store.uow.Consumer;
import nherald.indigo.store.uow.Transaction;

public class FileStore implements Store
{
    private final String root;

    private final ObjectMapper mapper;

    public FileStore(String root)
    {
        this.root = root;
        mapper = new ObjectMapper();
    }

    @Override
    public <T>T get(String namespace, String id, Class<T> itemType)
    {
        return get(namespace, Arrays.asList(id), itemType).get(0);
    }

    @Override
    public <T> List<T> get(String namespace, Collection<String> ids, Class<T> itemType)
    {
        return ids.stream()
            .map(IdValidator::check)
            .map(id -> read(namespace, id, itemType))
            .collect(Collectors.toList());
    }

    @Override
    public boolean exists(String namespace, String id)
    {
        IdValidator.check(id);

        return getFile(namespace, id).exists();
    }

    <T> void put(String namespace, String id, final T entity)
    {
        IdValidator.check(id);

        final File file = getFile(namespace, id);
        try
        {
            mapper.writeValue(file, entity);
        }
        catch (IOException e)
        {
            throw new StoreException("Error writing to " + file.getAbsolutePath(), e);
        }
    }

    void delete(String namespace, String id)
    {
        IdValidator.check(id);

        final File file = getFile(namespace, id);

        if (!file.delete())
        {
            throw new StoreException("Unable to delete file " + file.getAbsolutePath());
        }
    }

    @Override
    public void transaction(Consumer<Transaction> runnable)
    {
        final FileTransaction transaction = new FileTransaction(this);

        runnable.run(transaction);

        transaction.commit();
    }

    private File getFile(String namespace, String id)
    {
        return new File(root, String.format("%s-%s.json", namespace, id));
    }

    private <T> T read(String namespace, String id, Class<T> itemType)
    {
        final File file = getFile(namespace, id);

        if (!file.exists()) return null;

        try
        {
            return mapper.readValue(file, itemType);
        }
        catch (IOException e)
        {
            throw new StoreException("Error reading file " + file.getAbsolutePath(), e);
        }
    }
}
