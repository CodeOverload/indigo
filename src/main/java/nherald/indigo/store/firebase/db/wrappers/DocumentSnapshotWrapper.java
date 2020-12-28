package nherald.indigo.store.firebase.db.wrappers;

import com.google.cloud.firestore.DocumentSnapshot;

import nherald.indigo.store.firebase.db.FirebaseDocument;

public class DocumentSnapshotWrapper implements FirebaseDocument
{
    private final DocumentSnapshot document;

    public DocumentSnapshotWrapper(DocumentSnapshot document)
    {
        this.document = document;
    }

    @Override
    public boolean exists()
    {
        return document.exists();
    }

    @Override
    public <T> T asObject(Class<T> type)
    {
        return document.toObject(type);
    }
}
