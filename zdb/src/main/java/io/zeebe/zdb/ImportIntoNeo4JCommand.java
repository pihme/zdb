/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.zdb;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;
import static org.neo4j.driver.Values.parameters;

import io.zeebe.engine.state.deployment.PersistedWorkflow;
import io.zeebe.engine.state.instance.KeyIncidentPair;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.zdb.impl.IncidentInspection;
import io.zeebe.zdb.impl.PartitionState;
import io.zeebe.zdb.impl.WorkflowInspection;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;
import picocli.CommandLine.Spec;

@Command(name = "neo4j", mixinStandardHelpOptions = true, description = "Imports data into Neo4J")
public class ImportIntoNeo4JCommand implements Callable<Integer> {

  @Spec private CommandSpec spec;

  @Option(
      names = {"-p", "--path"},
      paramLabel = "PARTITION_PATH",
      description = "The path to the partition data (either runtime or snapshot in partition dir)",
      required = true,
      scope = ScopeType.INHERIT)
  private Path partitionPath;

  Driver driver = GraphDatabase.driver("bolt://127.0.0.1:7687", AuthTokens.basic("neo4j", "admin"));

  @Override
  public Integer call() {
    final var partitionState = PartitionState.of(partitionPath);

    dumpDatabase();

    new WorkflowInspection().forEach(partitionState, this::consume);
    new IncidentInspection().forEach(partitionState, this::consume);

    driver.close();
    return 0;
  }

  private void dumpDatabase() {

    try (Session session = driver.session()) {
      session.writeTransaction(
          new TransactionWork<Void>() {
            @Override
            public Void execute(Transaction tx) {
              tx.run("MATCH (n) DETACH DELETE n");

              return null;
            }
          });
    }
  }

  private void consume(KeyIncidentPair keyIncidentPair) {

    final IncidentRecord incidentRecord = keyIncidentPair.getIncident().getRecord();

    try (Session session = driver.session()) {
      session.writeTransaction(
          new TransactionWork<Void>() {
            @Override
            public Void execute(Transaction tx) {
              tx.run(
                  "CREATE (a:Incident) "
                      + "SET a.key = $key,"
                      + "a.workflowkey= $workflowKey,"
                      + "a.errorType= $errorType,"
                      + "a.errorMessage= $errorMessage",
                  parameters(
                      "key",
                      keyIncidentPair.getKey(),
                      "workflowKey",
                      incidentRecord.getWorkflowKey(),
                      "errorType",
                      incidentRecord.getErrorType().name(),
                      "errorMessage",
                      incidentRecord.getErrorMessage()));

              tx.run(
                  "MATCH (a:Incident), (b: Workflow) "
                      + "WHERE a.key=$incidentKey AND b.key=$workflowKey "
                      + "CREATE (b)-[r:ACTIVE_INCIDENT]->(a)",
                  parameters(
                      "incidentKey",
                      keyIncidentPair.getKey(),
                      "workflowKey",
                      incidentRecord.getWorkflowKey()));

              return null;
            }
          });
    }
  }

  private void consume(PersistedWorkflow workflow) {

    try (Session session = driver.session()) {
      session.writeTransaction(
          new TransactionWork<Void>() {
            @Override
            public Void execute(Transaction tx) {
              tx.run(
                  "CREATE (a:Workflow) "
                      + "SET a.key = $key,"
                      + "a.bpmnProcessId= $processId,"
                      + "a.version= $version,"
                      + "a.resourceName= $resourceName",
                  parameters(
                      "key",
                      workflow.getKey(),
                      "processId",
                      bufferAsString(workflow.getBpmnProcessId()),
                      "version",
                      workflow.getVersion(),
                      "resourceName",
                      bufferAsString(workflow.getResourceName())));
              return null;
            }
          });
    }
  }
}
