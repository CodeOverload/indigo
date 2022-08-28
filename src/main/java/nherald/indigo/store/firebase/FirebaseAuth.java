package nherald.indigo.store.firebase;

import java.io.IOException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;

public class FirebaseAuth
{
    /**
     * Authorises using the default Google credentials for the environment. Should
     * only be done once, before any firestore requests are made
     * @throws FirebaseAuthException if there was an error reading credentials
     */
    public void authDefault()
    {
        FirebaseApp.initializeApp();
    }

    /**
     * Authorises against a firestore project. Should only be done once, before any firestore
     * requests are made
     * @param projectId GCP project id that contains the firestore database
     * @throws FirebaseAuthException if there was an error reading credentials
     */
    public void auth(String projectId)
    {
        final GoogleCredentials credentials = getGoogleCredentials();
        final FirebaseOptions options = FirebaseOptions.builder()
            .setCredentials(credentials)
            .setProjectId(projectId)
            .build();

        FirebaseApp.initializeApp(options);
    }

    private static GoogleCredentials getGoogleCredentials()
    {
        try
        {
            return GoogleCredentials.getApplicationDefault();
        }
        catch (IOException ex)
        {
            throw new FirebaseAuthException("Error getting google credentials", ex);
        }
    }

    public static class FirebaseAuthException extends RuntimeException
    {
        private static final long serialVersionUID = 1L;

        public FirebaseAuthException(String message, Throwable t)
        {
            super(message, t);
        }
    }
}
