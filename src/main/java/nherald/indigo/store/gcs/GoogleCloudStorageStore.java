package nherald.indigo.store.gcs;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import nherald.indigo.store.IdValidator;
import nherald.indigo.store.Store;
import nherald.indigo.store.StoreException;
import nherald.indigo.uow.Transaction;

/**
 * Uses GCS buckets/objects to store entities. This was just written
 * for a POC to check the performance, so was never finished. It currently
 * doesn't take namespace into account, and most of the methods aren't
 * implemented
 */
public class GoogleCloudStorageStore implements Store
{
    private final Storage storage;
    private final ObjectMapper mapper;

    public GoogleCloudStorageStore()
    {
        storage = StorageOptions.getDefaultInstance().getService();
        mapper = new ObjectMapper();
    }

    @Override
    public <T> T get(String namespace, String id, Class<T> itemType)
    {
        IdValidator.check(id);

        final String filename = String.format("%s.json", id);

        // TODO use readAllBytes (as below, and consolidate into a helper method)
        final BlobId blobId = BlobId.of("project-id-here", filename);
        final Blob blob = storage.get(blobId);
        byte[] content = blob.getContent();

        return convert(filename, content, itemType); // mapper.readValue(content, itemType); // TODO encoding?
    }

    @Override
    public <T> List<T> get(String namespace, Collection<String> ids, Class<T> itemType)
    {
        return ids.stream()
            .map(IdValidator::check)
            .map(id -> {
                final String filename = String.format("%s.json", id); // TODO move into method
                final byte[] content = storage.readAllBytes("project-id-here", filename);
                return convert(filename, content, itemType);
            })
            .collect(Collectors.toList());
    }

    @Override
    public boolean exists(String namespace, String id)
    {
        IdValidator.check(id);

        // TODO
        return false;
    }

    @Override
    public <T> void put(String namespace, String id, T item, Transaction transaction)
    {
        IdValidator.check(id);

        // TODO
    }

    public void delete(String namespace, String id, Transaction transaction)
    {
        IdValidator.check(id);

        // TODO
    }

    @Override
    public Transaction transaction()
    {
        // TODO
        return null;
    }

    private <T> T convert(String filename, byte[] bytes, Class<T> itemType)
    {
        try
        {
            return mapper.readValue(bytes, itemType);
        }
        catch (IOException e)
        {
            throw new StoreException("Error reading " + filename, e);
        }
    }
}
