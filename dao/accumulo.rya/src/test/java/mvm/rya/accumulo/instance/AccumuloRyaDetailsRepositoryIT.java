package mvm.rya.accumulo.instance;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;

import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import mvm.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import mvm.rya.api.instance.RyaDetails.GeoIndexDetails;
import mvm.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import mvm.rya.api.instance.RyaDetails.ProspectorDetails;
import mvm.rya.api.instance.RyaDetails.TemporalIndexDetails;
import mvm.rya.api.instance.RyaDetailsRepository;
import mvm.rya.api.instance.RyaDetailsRepository.AlreadyInitializedException;
import mvm.rya.api.instance.RyaDetailsRepository.ConcurrentUpdateException;
import mvm.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import mvm.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;

/**
 * Tests the methods of {@link AccumuloRyaDetailsRepository} by using a {@link MiniAccumuloCluster}.
 */
public class AccumuloRyaDetailsRepositoryIT {

    private static MiniAccumuloCluster cluster = null;
    private static List<String> originalTableNames = new ArrayList<>();

    @BeforeClass
    public static void startMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        // Setup the mini cluster.
        final File tempDirectory = Files.createTempDirectory("testDir").toFile();
        cluster = new MiniAccumuloCluster(tempDirectory, "password");
        cluster.start();

        // Store a list of the original table names.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector("root", new PasswordToken("password"));
        originalTableNames.addAll( connector.tableOperations().list() );
    }

    @Before
    public void clearLastTest() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // Get a list of the tables that have been added by the test.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector("root", new PasswordToken("password"));
        final TableOperations tableOps = connector.tableOperations();

        final List<String> newTables = new ArrayList<>();
        newTables.addAll( tableOps.list() );
        newTables.removeAll( originalTableNames );

        // Delete all the new tables.
        for(final String newTable : newTables) {
            tableOps.delete( newTable );
        }
    }

    @AfterClass
    public static void stopMiniAccumulo() throws IOException, InterruptedException {
        cluster.stop();
    }

    @Test
    public void initializeAndGet() throws AccumuloException, AccumuloSecurityException, AlreadyInitializedException, RyaDetailsRepositoryException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(true) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(true) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() )
                                    .build())
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)
                                    .build())
                        .build())
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of Accumulo.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector("root", new PasswordToken("password"));
        final RyaDetailsRepository repo = new AccumuloRyaInstanceDetailsRepository(connector, instanceName);

        // Initialize the repository
        repo.initialize(details);

        // Fetch the stored details.
        final RyaDetails stored = repo.getRyaInstanceDetails();

        // Ensure the fetched object is equivalent to what was stored.
        assertEquals(details, stored);
    }

    @Test(expected = AlreadyInitializedException.class)
    public void initialize_alreadyInitialized() throws AlreadyInitializedException, RyaDetailsRepositoryException, AccumuloException, AccumuloSecurityException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(true) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(true) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() )
                                    .build())
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)
                                    .build())
                        .build())
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of Accumulo.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector("root", new PasswordToken("password"));
        final RyaDetailsRepository repo = new AccumuloRyaInstanceDetailsRepository(connector, instanceName);

        // Initialize the repository
        repo.initialize(details);

        // Initialize it again.
        repo.initialize(details);
    }

    @Test(expected = NotInitializedException.class)
    public void getRyaInstance_notInitialized() throws AccumuloException, AccumuloSecurityException, NotInitializedException, RyaDetailsRepositoryException {
        // Setup the repository that will be tested using a mock instance of Accumulo.
        final Instance instance = new MockInstance();
        final Connector connector = instance.getConnector("username", new PasswordToken("password"));
        final RyaDetailsRepository repo = new AccumuloRyaInstanceDetailsRepository(connector, "testInstance");

        // Try to fetch the details from the uninitialized repository.
        repo.getRyaInstanceDetails();
    }

    @Test
    public void isInitialized_true() throws AccumuloException, AccumuloSecurityException, AlreadyInitializedException, RyaDetailsRepositoryException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(true) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(true) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() )
                                    .build())
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)
                                    .build())
                        .build())
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of Accumulo.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector("root", new PasswordToken("password"));
        final RyaDetailsRepository repo = new AccumuloRyaInstanceDetailsRepository(connector, "testInstance");

        // Initialize the repository
        repo.initialize(details);

        // Ensure the repository reports that it has been initialized.
        assertTrue( repo.isInitialized() );
    }

    @Test
    public void isInitialized_false() throws AccumuloException, AccumuloSecurityException, RyaDetailsRepositoryException {
        // Setup the repository that will be tested using a mock instance of Accumulo.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector("root", new PasswordToken("password"));
        final RyaDetailsRepository repo = new AccumuloRyaInstanceDetailsRepository(connector, "testInstance");

        // Ensure the repository reports that is has not been initialized.
        assertFalse( repo.isInitialized() );
    }

    @Test
    public void update() throws AlreadyInitializedException, RyaDetailsRepositoryException, AccumuloException, AccumuloSecurityException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(true) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(true) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() )
                                    .build())
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)
                                    .build())
                        .build())
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of Accumulo.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector("root", new PasswordToken("password"));
        final RyaDetailsRepository repo = new AccumuloRyaInstanceDetailsRepository(connector, "testInstance");

        // Initialize the repository
        repo.initialize(details);

        // Create a new state for the details.
        final RyaDetails updated = new RyaDetails.Builder( details )
                .setGeoIndexDetails( new GeoIndexDetails(false) )
                .build();

        // Execute the update.
        repo.update(details, updated);

        // Show the new state that is stored matches the updated state.
        final RyaDetails fetched = repo.getRyaInstanceDetails();
        assertEquals(updated, fetched);
    }

    @Test(expected = ConcurrentUpdateException.class)
    public void update_outOfDate() throws AccumuloException, AccumuloSecurityException, AlreadyInitializedException, RyaDetailsRepositoryException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
            .setGeoIndexDetails( new GeoIndexDetails(true) )
            .setTemporalIndexDetails( new TemporalIndexDetails(true) )
            .setFreeTextDetails( new FreeTextIndexDetails(true) )
            .setPCJIndexDetails(
                    PCJIndexDetails.builder()
                        .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 1")
                                    .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                    .setLastUpdateTime( new Date() )
                                    .build())
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")
                                    .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)
                                    .build())
                        .build())
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of Accumulo.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector("root", new PasswordToken("password"));
        final RyaDetailsRepository repo = new AccumuloRyaInstanceDetailsRepository(connector, "testInstance");

        // Initialize the repository
        repo.initialize(details);

        // Create a new state for the details.
        final RyaDetails updated = new RyaDetails.Builder( details )
                .setGeoIndexDetails( new GeoIndexDetails(false) )
                .build();

        // Try to execute the update where the old state is not the currently stored state.
        repo.update(updated, updated);
    }
}