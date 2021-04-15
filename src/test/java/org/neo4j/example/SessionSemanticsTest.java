package org.neo4j.example;

import static com.neo4j.harness.EnterpriseNeo4jBuilders.newInProcessBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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

    private static final String CREATE_AND_RETURN_USER = "CREATE (n:Person) SET n.name = $name RETURN n.name";
    private static final String MATCH_AND_RETURN_USER = "MATCH (n:Person) WHERE n.name = $name RETURN n.name";

    @BeforeAll
    public static void setUp() throws Exception {
        // start up a new neo4j server with authentication disabled
        server = newInProcessBuilder().build();
    }

    @AfterAll
    public static void tearDown() {
        server.close();
    }

    @BeforeEach
    public void detachDelete() {
        // every test will start with a clean slate where only alice exists
        GraphDatabaseService graphDb = server.databaseManagementService().database("neo4j");
        graphDb.executeTransactionally("CREATE CONSTRAINT person_name IF NOT EXISTS ON (n:Person) ASSERT (n.name) IS NODE KEY");
        graphDb.executeTransactionally("MATCH (n:Person) WHERE n.name <> 'alice' DETACH DELETE n");
        graphDb.executeTransactionally("MERGE (n:Person) SET n.name = 'alice' RETURN 1");
    }

    @Test
    public void autoCommit() {

        // an auto-commit transaction is one that uses session.run()
        // auto-commit transactions are not automatically retried by the driver upon transient errors, like transaction functions are
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                Value single = session.run(CREATE_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));

                try {
                    // calling single() always exhausts the result
                    session.run(CREATE_AND_RETURN_USER, parameters("name", "alice")).single();
                    assertThat("The previous code should have thrown an exception.", false);
                } catch (ClientException e) {
                    assertThat(e.getMessage(), containsString("already exists with label"));
                }
            }

            // bob exists
            try (Session session = driver.session()) {
                Value single = session.run(MATCH_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));
            }
        }
    }

    @Test
    public void autoCommitWithUnconsumedResults() {
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                Value single = session.run(CREATE_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));

                // the difference here is that we don't consume the result set so we don't get the exception until closing the session
                session.run(CREATE_AND_RETURN_USER, parameters("name", "alice"));
            } catch (ClientException e) {
                assertThat(e.getMessage(), containsString("already exists with label"));
            }

            // bob exists
            try (Session session = driver.session()) {
                Value single = session.run(MATCH_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));
            }
        }
    }

    @Test
    public void transactionFunction() {

        // a transaction function is one that uses session.writeTransaction() or session.readTransaction()
        // transaction functions will automatically be committed unless an exception is thrown during query execution or by the user code
        // transaction functions will be automatically retried by the driver in the case of transient errors
        // transaction functions must always use idempotent cypher queries because the retry logic does not roll back first
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                session.writeTransaction(tx -> {
                    Value single = tx.run(CREATE_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));
                    return 1;
                });

                try {
                    session.writeTransaction(tx -> {
                        tx.run(CREATE_AND_RETURN_USER, parameters("name", "alice"));
                        return 1;
                    });
                    assertThat("The previous code should have thrown an exception.", false);
                } catch (ClientException e) {
                    assertThat(e.getMessage(), containsString("already exists with label"));
                }
            }

            // bob exists
            try (Session session = driver.session()) {
                session.readTransaction(tx -> {
                    Value single = tx.run(MATCH_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
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
                        tx.run(CREATE_AND_RETURN_USER, parameters("name", "alice"));
                        return 1;
                    });
                    assertThat("The previous code should have thrown an exception.", false);
                } catch (ClientException e) {
                    assertThat(e.getMessage(), containsString("already exists with label"));
                }

                session.writeTransaction(tx -> {
                    Value single = tx.run(CREATE_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));
                    return 1;
                });
            }

            // bob exists
            try (Session session = driver.session()) {
                session.readTransaction(tx -> {
                    Value single = tx.run(MATCH_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));
                    return 1;
                });
            }
        }
    }

    @Test
    public void transactionFunctionMultiQuery() {

        // this is just an example of a transaction function that runs multiple queries inside of it
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                session.writeTransaction(tx -> {
                    Value single = tx.run(CREATE_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));

                    single = tx.run(CREATE_AND_RETURN_USER, parameters("name", "claudia")).single().get(0);
                    assertThat(single.asString(), equalTo("claudia"));
                    return 1;
                });
            }

            // bob and claudia both exist
            try (Session session = driver.session()) {
                session.readTransaction(tx -> {
                    Value single = tx.run(MATCH_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));

                    single = tx.run(MATCH_AND_RETURN_USER, parameters("name", "claudia")).single().get(0);
                    assertThat(single.asString(), equalTo("claudia"));
                    return 1;
                });
            }
        }
    }

    @Test
    public void transactionFunctionMultiQueryWithRollback() {

        // here we simply demonstrate that the entire transaction is rolled back when a non-transient error occurs
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                session.writeTransaction(tx -> {
                    Value single = tx.run(CREATE_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));

                    try {
                        tx.run(CREATE_AND_RETURN_USER, parameters("name", "alice")).single();
                        assertThat("The previous code should have thrown an exception.", false);
                    } catch (ClientException e) {
                        assertThat(e.getMessage(), containsString("already exists with label"));
                    }
                    return 1;
                });
            } catch (ClientException e) {
                assertThat(e.getMessage(), containsString("Transaction can't be committed. It has been rolled back"));
            }

            // bob does not exist
            try (Session session = driver.session()) {
                session.readTransaction(tx -> {
                    Result result = tx.run(MATCH_AND_RETURN_USER, parameters("name", "bob"));
                    assertThat(result.hasNext(), is(false));
                    return 1;
                });
            }
        }
    }

    @Test
    public void transactionFunctionMultiQueryWithUserException() {

        // when a user exception is thrown in a transaction function, the entire transaction is rolled back
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                session.writeTransaction(tx -> {
                    Value single = tx.run(CREATE_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                    assertThat(single.asString(), equalTo("bob"));

                    @SuppressWarnings("unused")
                    float error = 1 / 0;

                    single = tx.run(CREATE_AND_RETURN_USER, parameters("name", "claudia")).single().get(0);
                    assertThat(single.asString(), equalTo("claudia"));
                    return 1;
                });
            } catch (ArithmeticException e) {
            }

            // neither bob nor claudia exist
            try (Session session = driver.session()) {
                session.readTransaction(tx -> {
                    Result result = tx.run(MATCH_AND_RETURN_USER, parameters("name", "bob"));
                    assertThat(result.hasNext(), is(false));
                    return 1;
                });

                session.readTransaction(tx -> {
                    Result result = tx.run(MATCH_AND_RETURN_USER, parameters("name", "claudia"));
                    assertThat(result.hasNext(), is(false));
                    return 1;
                });
            }
        }
    }

    @Test
    public void unmanagedTransactionAutoRollbackWithoutCommit() {

        // an unmanaged transaction is one that is created with session.beginTransaction()
        // unmanaged transactions should be committed and closed manually
        // unmanaged transactions will be rolled back by default in close, if it was not committed or rolled back before close
        // unmanaged transactions do not need to be idempotent because there is no automatic retry logic in the driver
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                Transaction tx = session.beginTransaction();
                Value single = tx.run(CREATE_AND_RETURN_USER, parameters("name", "bob")).single().get(0);
                assertThat(single.asString(), equalTo("bob"));
                tx.close();
            }

            // bob does not exist
            try (Session session = driver.session()) {
                Transaction tx = session.beginTransaction();
                Result result = tx.run(MATCH_AND_RETURN_USER, parameters("name", "bob"));
                assertThat(result.hasNext(), is(false));
                tx.close();
            }
        }
    }

    @Test
    public void unmanagedTransactionConsumeAfterCommit() {

        // when using unmanaged transactions we can't consume the results after commit
        try (Driver driver = GraphDatabase.driver(server.boltURI(), AuthTokens.basic("neo4j", ""), config)) {

            try (Session session = driver.session()) {
                Transaction tx = session.beginTransaction();
                Result result = tx.run(CREATE_AND_RETURN_USER, parameters("name", "bob"));
                tx.commit();

                try {
                    result.single().get(0);
                    assertThat("The previous code should have thrown an exception.", false);
                } catch (ResultConsumedException e) {
                    assertThat(e.getMessage(), containsString("Cannot access records on this result any more as the result has already been consumed"));
                }
                tx.close();
            }
        }
    }
}
