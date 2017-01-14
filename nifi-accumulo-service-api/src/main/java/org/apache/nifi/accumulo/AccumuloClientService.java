/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.accumulo;

import org.apache.nifi.accumulo.put.PutFlowFile;
import org.apache.nifi.accumulo.put.PutMutation;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.Collection;

@Tags({"Accumulo, Client"})
@CapabilityDescription("A controller service for accessing an Accumulo connector")
public interface AccumuloClientService extends ControllerService {

    public static final PropertyDescriptor ZOOKEEPER_QUORUM = new PropertyDescriptor.Builder()
            .name("ZooKeeper Quorum")
            .description("Comma-separated list of ZooKeeper hosts for Accumulo. Required if Hadoop Configuration Files are not provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
            .name("Instance Name")
            .description("All clients must first identify the Accumulo instance to which they will be communicating.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The user name for connecting to Accumulo")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The Password for connecting to Accumulo")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    /**
     * Puts a batch of mutations to the given table.
     *
     * @param tableName the name of an Accumulo table
     * @param mutations      a list of put mutations for the given table
     * @throws IOException thrown when there are communication errors with Accumulo
     */
    void put(String tableName, Collection<PutFlowFile> mutations) throws IOException;

    /**
     * Puts the given mutation to Accumulo with the provided columns.
     *
     * @param tableName the name of an Accumulo table
     * @param rowId     the id of the row to put
     * @param mutations the columns of the row to put
     * @throws IOException thrown when there are communication errors with Accumulo
     */
    void put(String tableName, String rowId, Collection<PutMutation> mutations) throws IOException;

    /**
     * Converts the given boolean to it's byte representation.
     *
     * @param b a boolean
     * @return the boolean represented as bytes
     */
    byte[] toBytes(boolean b);

    /**
     * Converts the given long to it's byte representation.
     *
     * @param l a long
     * @return the long represented as bytes
     */
    byte[] toBytes(long l);

    /**
     * Converts the given double to it's byte representation.
     *
     * @param d a double
     * @return the double represented as bytes
     */
    byte[] toBytes(double d);

    /**
     * Converts the given string to it's byte representation.
     *
     * @param s a string
     * @return the string represented as bytes
     */
    byte[] toBytes(String s);

    /**
     * Converts the given binary formatted string to a byte representation
     * @param s a binary encoded string
     * @return the string represented as bytes
     */
    byte[] toBytesBinary(String s);


}
