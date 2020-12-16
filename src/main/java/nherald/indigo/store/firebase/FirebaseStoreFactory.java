package nherald.indigo.store.firebase;

import nherald.indigo.store.Store;
import nherald.indigo.store.StoreFactory;

public class FirebaseStoreFactory implements StoreFactory
{
    @Override
    public Store get()
    {
        return new FirebaseStore();
    }
}
