package nherald.indigo.store.firebase;

import com.google.cloud.firestore.Firestore;
import com.google.firebase.cloud.FirestoreClient;

import nherald.indigo.store.Store;
import nherald.indigo.store.StoreFactory;
import nherald.indigo.store.firebase.db.FirebaseDatabase;
import nherald.indigo.store.firebase.db.wrappers.FirestoreWrapper;

public class FirebaseStoreFactory implements StoreFactory
{
    @Override
    public Store get()
    {
        final Firestore firestore = FirestoreClient.getFirestore();
        final FirebaseDatabase database = new FirestoreWrapper(firestore);

        return new FirebaseStore(database);
    }
}
