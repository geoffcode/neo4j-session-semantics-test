package org.neo4j.example;

import static com.neo4j.harness.EnterpriseNeo4jBuilders.newInProcessBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.driver.Values.parameters;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.Neo4j;

public class SessionSemanticsTest {
    private static final Config config = Config.builder().withLogging(Logging.none()).withoutEncryption().build();
    private static Neo4j server;

    private static final String CREATE_RETURN_USER = "CREATE (n:Person) SET n.name = $name RETURN n.name";
    private static final String MATCH_RETURN_USER = "MATCH (n:Person) WHERE n.name = $name RETURN n.name";

    @BeforeAll
    public static void setUp() throws Exception {
        // start up a new neo4j server with authentication disabled
        server = newInProcessBuilder().build();

        GraphDatabaseService graphDb = server.databaseManagementService().database("neo4j");
        graphDb.executeTransactionally("CREATE CONSTRAINT person_name ON (n:Person) ASSERT (n.name) IS NODE KEY");
    }

    @AfterAll
    public static void tearDown() {
        server.close();
    }

    @BeforeEach
    public void detachDelete() {
        GraphDatabaseService graphDb = server.databaseManagementService().database("neo4j");
        graphDb.executeTransactionally("MATCH (n) WHERE n.name <> 'alice' DETACH DELETE n");
        graphDb.executeTransactionally("MERGE (n:Person) SET n.name = 'alice' RETURN 1");
    }

    @Test
    public void autoCommit() {
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                Value single = session.run(CREATE_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));

                try {
                    session.run(CREATE_RETURN_USER, parameters("name", "alice")).single();
                    assertThat("This should have failed.", false);
                } catch (ClientException e) {
                    assertThat(e.getMessage(), containsString("already exists with label"));
                }
            }

            try (Session session = driver.session()) {
                Value single = session.run(MATCH_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));
            }
        }
    }

    @Test
    public void autoCommitWithUnconsumedResults() {
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                Value single = session.run(CREATE_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));

                session.run(CREATE_RETURN_USER, parameters("name", "alice"));
            } catch (ClientException e) {
                assertThat(e.getMessage(), containsString("already exists with label"));
            }

            try (Session session = driver.session()) {
                Value single = session.run(MATCH_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));
            }
        }
    }

    @Test
    public void transactionFunction() {

        // per https://neo4j.com/docs/api/java-driver/current/org/neo4j/driver/Session.html#writeTransaction-org.neo4j.driver.TransactionWork-
        // This transaction will automatically be committed unless an exception is thrown during query execution or by the user code.

        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                session.writeTransaction(tx -> {
                    Value single = tx.run(CREATE_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));
                    return 1;
                });

                try {
                    session.writeTransaction(tx -> {
                        tx.run(CREATE_RETURN_USER, parameters("name", "alice"));
                        return 1;
                    });
                    assertThat("This should have failed.", false);
                } catch (ClientException e) {
                    assertThat(e.getMessage(), containsString("already exists with label"));
                }
            }

            try (Session session = driver.session()) {
                session.readTransaction(tx -> {
                    Value single = tx.run(MATCH_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));
                    return 1;
                });
            }
        }
    }

    @Test
    public void transactionFunctionReverseOrder() {
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                try {
                    session.writeTransaction(tx -> {
                        tx.run(CREATE_RETURN_USER, parameters("name", "alice"));
                        return 1;
                    });
                    assertThat("This should have failed.", false);
                } catch (ClientException e) {
                    assertThat(e.getMessage(), containsString("already exists with label"));
                }

                session.writeTransaction(tx -> {
                    Value single = tx.run(CREATE_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));
                    return 1;
                });
            }

            try (Session session = driver.session()) {
                session.readTransaction(tx -> {
                    Value single = tx.run(MATCH_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));
                    return 1;
                });
            }
        }
    }

    @Test
    public void transactionFunctionMultiQuery() {
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                try {
                    session.writeTransaction(tx -> {
                        tx.run(CREATE_RETURN_USER, parameters("name", "alice"));
                        return 1;
                    });
                    assertThat("This should have failed.", false);
                } catch (ClientException e) {
                    assertThat(e.getMessage(), containsString("already exists with label"));
                }

                session.writeTransaction(tx -> {
                    Value single = tx.run(CREATE_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));

                    single = tx.run(CREATE_RETURN_USER, parameters("name", "claudia")).single().get(0);
                    assertThat(single.asString(), equalTo("claudia"));

                    return 1;
                });
            }

            try (Session session = driver.session()) {
                session.readTransaction(tx -> {
                    Value single = tx.run(MATCH_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));

                    single = tx.run(MATCH_RETURN_USER, parameters("name", "claudia")).single().get(0);
                    assertThat(single.asString(), equalTo("claudia"));

                    return 1;
                });
            }
        }
    }

    @Test
    public void transactionFunctionMultiQueryFailRollback() {
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try {
                try (Session session = driver.session()) {
                    session.writeTransaction(tx -> {
                        Value single = tx.run(CREATE_RETURN_USER, parameters("name", "bob")).single().get(0);
                        assertThat(single.asString(), equalTo("bob"));
                        try {
                            tx.run(CREATE_RETURN_USER, parameters("name", "alice")).single();
                            assertThat("This should have failed.", false);
                        } catch (ClientException e) {
                            assertThat(e.getMessage(), containsString("already exists with label"));
                        }

                        return 1;
                    });
                }
            } catch (ClientException e) {
                assertThat(e.getMessage(), containsString("Transaction can't be committed. It has been rolled back"));
            }

            try (Session session = driver.session()) {
                session.readTransaction(tx -> {
                    Result result = tx.run(MATCH_RETURN_USER, parameters("name", "bob"));
                    assertThat(result.hasNext(), equalTo(false));

                    return 1;
                });
            }
        }
    }

    @Test
    public void unmanagedTransactionAutoRollbackWithoutCommit() {

        // tx.commit()
        // Commit this current transaction. When this method returns, all outstanding queries in the transaction are guaranteed to have completed, meaning any writes you performed are guaranteed to be durably
        // stored. No more queries can be executed inside this transaction once this transaction is committed. After this method is called, the transaction cannot be committed or rolled back again. You must
        // call this method before calling close() to have your transaction committed. If a transaction is not committed or rolled back before close, the transaction will be rolled back by default in close.

        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                Transaction tx = session.beginTransaction();
                Value single = tx.run(CREATE_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));
                tx.close();
            }

            try (Session session = driver.session()) {
                Transaction tx = session.beginTransaction();
                Result result = tx.run(MATCH_RETURN_USER, parameters("name", "bob"));
                assertThat(result.hasNext(), equalTo(false));
                tx.close();
            }
        }
    }

    @Test
    public void unmanagedTransactionConsumeAfterCommit() {

        // tx.commit()
        // Commit this current transaction. When this method returns, all outstanding queries in the transaction are guaranteed to have completed, meaning any writes you performed are guaranteed to be durably
        // stored. No more queries can be executed inside this transaction once this transaction is committed. After this method is called, the transaction cannot be committed or rolled back again. You must
        // call this method before calling close() to have your transaction committed. If a transaction is not committed or rolled back before close, the transaction will be rolled back by default in close.

        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {
            try (Session session = driver.session()) {

                Transaction tx = session.beginTransaction();
                Result result = tx.run(CREATE_RETURN_USER, parameters("name", "bob"));
                tx.commit();

                try {
                    result.single().get(0);
                } catch (ResultConsumedException e) {
                    assertThat(e.getMessage(), containsString("Cannot access records on this result any more as the result has already been consumed"));
                }
                tx.close();
            }
        }
    }
}
