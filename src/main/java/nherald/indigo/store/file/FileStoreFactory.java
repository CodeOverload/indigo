package nherald.indigo.store.file;

import nherald.indigo.store.Store;
import nherald.indigo.store.StoreFactory;

public class FileStoreFactory implements StoreFactory
{
    private final String rootDir;

    public FileStoreFactory(String rootDir)
    {
        this.rootDir = rootDir;
    }

    @Override
    public Store get()
    {
        return new FileStore(rootDir);
    }
}
