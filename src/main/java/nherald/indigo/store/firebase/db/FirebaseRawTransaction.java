package nherald.indigo.store.firebase.db;

public interface FirebaseRawTransaction
{
    <T> void set(FirebaseDocumentId id, T entity);

    void delete(FirebaseDocumentId id);
}
