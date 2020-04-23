package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    public static final String SESSION_COLLECTION = "sessions";
    public static final String USER_COLLECTION = "users";
    private final MongoCollection<User> usersCollection;

    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        log = LoggerFactory.getLogger(this.getClass());
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection(USER_COLLECTION, User.class).withCodecRegistry(pojoCodecRegistry);
        sessionsCollection = db.getCollection(SESSION_COLLECTION, Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        try {
            usersCollection.withWriteConcern(WriteConcern.W1.withWTimeout(2, TimeUnit.MICROSECONDS)).
                    insertOne(user);
            return true;
        } catch (Exception e) {
            throw new IncorrectDaoOperation("Couldn't insert user");
        }
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        Session session = new Session();
        session.setUserId(userId);
        session.setJwt(jwt);
        if (sessionsCollection.find(eq("user_id", userId)).first() != null) { //hotfix
            deleteUserSessions(userId);
        }
        return  sessionsCollection.insertOne(session).wasAcknowledged();
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        return usersCollection.find(eq("email", email)).first();
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        Bson query = eq("user_id", userId);
        return sessionsCollection.find(query).first();
    }

    public boolean deleteUserSessions(String userId) {
        DeleteResult result = null;
        try {
            result = sessionsCollection.deleteMany(eq("user_id", userId));
        } catch (Exception e) {
            log.error("The was an error while deleting user's sessions", e);
        }
        return result != null && result.getDeletedCount() > 0;
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        deleteUserSessions(email);
        DeleteResult result = null;
        try {
            result = usersCollection.deleteOne(eq("email", email));
        } catch (Exception e) {
            log.error("The was an error while deleting the user", e);
        }
        return result != null && result.getDeletedCount() > 0;
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        UpdateResult result;
        try {
            Document updatePrefs = new Document();
            userPreferences.forEach(updatePrefs::put);
            UpdateOptions options = new UpdateOptions();
            options.upsert(true);
            result = usersCollection.updateOne(eq("email", email),
                    set("preferences", updatePrefs),
                    options);
        } catch (Exception e) {
            throw new IncorrectDaoOperation("Couldn't update user preferences", e);
        }

        return result.getModifiedCount() != 0;
    }
}
