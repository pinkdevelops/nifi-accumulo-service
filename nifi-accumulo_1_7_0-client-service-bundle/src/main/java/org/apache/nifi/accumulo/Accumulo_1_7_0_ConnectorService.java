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

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.nifi.accumulo.put.PutFlowFile;
import org.apache.nifi.accumulo.put.PutMutation;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Tags({"Accumulo, Connection, Instance"})
@CapabilityDescription("Provides Accumulo connector to a given instance")
public class Accumulo_1_7_0_ConnectorService extends AbstractControllerService implements AccumuloClientService {

    private Connector connector;
    private List<PropertyDescriptor> properties;

    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ZOOKEEPER_QUORUM);
        props.add(INSTANCE_NAME);
        props.add(USER);
        props.add(PASSWORD);
        this.properties = Collections.unmodifiableList(props);

    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' in the Accumulo configuration.")
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        boolean zkQuorumProvided = validationContext.getProperty(ZOOKEEPER_QUORUM).isSet();
        boolean instanceNamerovided = validationContext.getProperty(INSTANCE_NAME).isSet();
        boolean userProvided = validationContext.getProperty(USER).isSet();
        boolean passwordProvided = validationContext.getProperty(PASSWORD).isSet();

        final List<ValidationResult> problems = new ArrayList<>();

        if (!zkQuorumProvided && (!instanceNamerovided || !userProvided || !passwordProvided)) {
            problems.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(this.getClass().getSimpleName())
                    .explanation("ZooKeeper Quorum, Instance Name, Accumulo User, and Accumulo Password are required " +
                            "to connect")
                    .build());
        }
        return problems;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, InterruptedException {

        String instanceName = context.getProperty(INSTANCE_NAME).getValue();
        String zkQuorum = context.getProperty(ZOOKEEPER_QUORUM).getValue();
        String accumuloUser = context.getProperty(USER).getValue();
        String accumuloPassword = context.getProperty(PASSWORD).getValue();

        Instance inst = new ZooKeeperInstance(instanceName, zkQuorum);

        try {
            this.connector = inst.getConnector(accumuloUser, new PasswordToken(accumuloPassword));
        } catch (AccumuloException e) {
            e.printStackTrace();
        } catch (AccumuloSecurityException e) {
            e.printStackTrace();
        }
    }

    @OnDisabled
    public void shutdown() {
        if (connector != null) {
            connector = null;
        }
    }

    @Override
    public void put(final String tableName, final Collection<PutFlowFile> puts) throws IOException {

        BatchWriter writer = batchWriter(10000000L, tableName);

        for (final PutFlowFile putFlowFile : puts) {
            for (final PutMutation column : putFlowFile.getColumns()) {
                Mutation mutation = new Mutation(new Text(putFlowFile.getRow()));
                mutation.put(new Text(column.getColumnFamily()), new Text(column.getColumnQualifier()),
                        new ColumnVisibility(column.getColumnVisibilty()), new Value(column.getBuffer().getBytes()));

                try {
                    writer.addMutation(mutation);
                } catch (MutationsRejectedException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            writer.close();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void put(final String tableName, final String rowId, final Collection<PutMutation> columns) throws IOException {

        BatchWriter writer = batchWriter(10000000L, tableName);

        Mutation mutation = new Mutation(new Text(rowId));
        for (final PutMutation column : columns) {
            mutation.put(new Text(column.getColumnFamily()), new Text(column.getColumnQualifier()),
                    new ColumnVisibility(column.getColumnVisibilty()), new Value(column.getBuffer().getBytes()));
        }

        try {
            writer.addMutation(mutation);
            writer.close();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
    }

    public BatchWriter batchWriter(long maxMemory, String tableName) {

        BatchWriterConfig bwConfig = new BatchWriterConfig();
        bwConfig.setMaxMemory(maxMemory);
        BatchWriter batchWriter = null;

        try {
            batchWriter = connector.createBatchWriter(tableName, bwConfig);
        } catch (TableNotFoundException e) {
            e.printStackTrace();
        }
        return batchWriter;
    }

    @Override
    public byte[] toBytes(boolean b) {
        byte[] vOut = new byte[]{(byte) (b ? 1 : 0)};
        return vOut;
    }

    @Override
    public byte[] toBytes(long l) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(l);
        return buffer.array();
    }

    @Override
    public byte[] toBytes(double d) {
        byte[] bytes = ByteBuffer.allocate(8).putDouble(d).array();
        return bytes;
    }

    @Override
    public byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] toBytesBinary(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
