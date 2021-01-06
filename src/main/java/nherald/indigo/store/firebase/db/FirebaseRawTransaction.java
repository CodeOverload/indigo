package nherald.indigo.store.firebase.db;

public interface FirebaseRawTransaction extends FirebaseReadOps
{
    <T> void set(FirebaseDocumentId id, T entity);

    void delete(FirebaseDocumentId id);
}
