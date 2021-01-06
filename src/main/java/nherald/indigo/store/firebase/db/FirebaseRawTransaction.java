package nherald.indigo.store.firebase.db;

public interface FirebaseRawTransaction extends FirebaseRawReadOps
{
    <T> void set(FirebaseRawDocumentId id, T entity);

    void delete(FirebaseRawDocumentId id);
}
