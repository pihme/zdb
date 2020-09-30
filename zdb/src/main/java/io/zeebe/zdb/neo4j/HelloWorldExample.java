/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.zdb.neo4j;

import static org.neo4j.driver.Values.parameters;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

public class HelloWorldExample implements AutoCloseable {
  private final Driver driver;

  public HelloWorldExample(String uri, String user, String password) {
    driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
  }

  @Override
  public void close() throws Exception {
    driver.close();
  }

  public void printGreeting(final String message) {
    try (Session session = driver.session()) {
      final String greeting =
          session.writeTransaction(
              new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                  final Result result =
                      tx.run(
                          "CREATE (a:Greeting) "
                              + "SET a.message = $message "
                              + "RETURN a.message + ', from node ' + id(a)",
                          parameters("message", message));
                  return result.single().get(0).asString();
                }
              });
      System.out.println(greeting);
    }
  }

  public static void main(String... args) throws Exception {
    try (HelloWorldExample greeter =
        new HelloWorldExample("bolt://127.0.0.1:7687", "neo4j", "admin")) {
      greeter.printGreeting("hello, world");
    }
  }
}
