package nherald.indigo.store.firebase;

import nherald.indigo.store.firebase.db.FirebaseDocument;

public class TestFirebaseDocument implements FirebaseDocument
{
    private final boolean exists;
    private final Object entity;

    public TestFirebaseDocument(boolean exists, Object entity)
    {
        this.exists = exists;
        this.entity = entity;
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T asObject(Class<T> type)
    {
        return (T) entity;
    }
}
