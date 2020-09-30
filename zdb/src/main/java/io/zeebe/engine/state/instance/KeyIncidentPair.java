/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.instance;

public class KeyIncidentPair {

  long key;

  Incident incident;

  public KeyIncidentPair(long key, Incident incident) {
    this.key = key;
    this.incident = incident;
  }

  public long getKey() {
    return key;
  }

  public Incident getIncident() {
    return incident;
  }
}
