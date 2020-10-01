/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.geotemporal.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoRyaSailFactory;
import org.apache.rya.indexing.OptionalConfigUtils;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class AccumuloGeoTemporalIndexIT extends AccumuloITBase {
    private static final String URI_PROPERTY_AT_TIME = "Property:atTime";

    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
    private SailRepositoryConnection conn;
    private AccumuloGeoTemporalIndexer indexer;
    private AccumuloRdfConfiguration ryaConf;
	private static final String RYA_INSTANCE_NAME = "testInstance";
	private static final GeometryFactory GF = new GeometryFactory(new PrecisionModel(), 4326);

    @Before
    public void setUp() throws Exception{
		ryaConf = new AccumuloRdfConfiguration();
		ryaConf.setTablePrefix(RYA_INSTANCE_NAME);
		ryaConf.set(ConfigUtils.CLOUDBASE_USER, super.getUsername());
		ryaConf.set(ConfigUtils.CLOUDBASE_PASSWORD, super.getPassword());
		ryaConf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, super.getZookeepers());
		ryaConf.set(ConfigUtils.CLOUDBASE_INSTANCE, super.getInstanceName());
		ryaConf.set(OptionalConfigUtils.USE_GEO, "false");
		ryaConf.set(OptionalConfigUtils.USE_GEOTEMPORAL, "true");
		ryaConf.set(OptionalConfigUtils.USE_MONGO, "false");
		
        final Sail sail = GeoRyaSailFactory.getInstance(ryaConf);
        conn = new SailRepository(sail).getConnection();
        conn.begin();

        indexer = new AccumuloGeoTemporalIndexer();
        indexer.setConf(ryaConf);
        indexer.init();
    }

    @Test
    public void ensureInEventStore_Test() throws Exception {
        addStatements();

        final EventStorage events = indexer.getEventStorage(ryaConf);
        final RyaURI subject = new RyaURI("urn:event1");
        final Optional<Event> event = events.get(subject);
        assertTrue(event.isPresent());
    }

    @Test
    public void constantSubjQuery_Test() throws Exception {
        addStatements();
        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT * "
              + "WHERE { "
                + "  <urn:event1> time:atTime ?time . "
                + "  <urn:event1> geo:asWKT ?point . "
                + "  FILTER(geof:sfWithin(?point, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                + "  FILTER(tempo:equals(?time, \"2015-12-30T12:00:00Z\")) "
              + "}";

        final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        final Set<BindingSet> results = new HashSet<>();
        while(rez.hasNext()) {
            final BindingSet bs = rez.next();
            results.add(bs);
        }
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("point", VF.createLiteral("POINT (0 0)"));
        expected.addBinding("time", VF.createLiteral("2015-12-30T07:00:00-05:00"));

        assertEquals(1, results.size());
        assertEquals(expected, results.iterator().next());
    }

    @Test
    public void variableSubjQuery_Test() throws Exception {
        addStatements();
        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT * "
              + "WHERE { "
                + "  ?subj time:atTime ?time . "
                + "  ?subj geo:asWKT ?point . "
                + "  FILTER(geof:sfWithin(?point, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                + "  FILTER(tempo:equals(?time, \"2015-12-30T12:00:00Z\")) "
              + "}";

        final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        final List<BindingSet> results = new ArrayList<>();
        while(rez.hasNext()) {
            final BindingSet bs = rez.next();
            results.add(bs);
        }
        final MapBindingSet expected1 = new MapBindingSet();
        expected1.addBinding("point", VF.createLiteral("POINT (0 0)"));
        expected1.addBinding("time", VF.createLiteral("2015-12-30T07:00:00-05:00"));

        final MapBindingSet expected2 = new MapBindingSet();
        expected2.addBinding("point", VF.createLiteral("POINT (1 1)"));
        expected2.addBinding("time", VF.createLiteral("2015-12-30T07:00:00-05:00"));

        assertEquals(2, results.size());
        assertEquals(expected1, results.get(0));
        assertEquals(expected2, results.get(1));
    }

    private void addStatements() throws Exception {
        URI subject = VF.createURI("urn:event1");
        final URI predicate = VF.createURI(URI_PROPERTY_AT_TIME);
        Value object = VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        conn.add(VF.createStatement(subject, predicate, object));

        object = VF.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        conn.add(VF.createStatement(subject, GeoConstants.GEO_AS_WKT, object));

        subject = VF.createURI("urn:event2");
        object = VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        conn.add(VF.createStatement(subject, predicate, object));

        object = VF.createLiteral("Point(1 1)", GeoConstants.XMLSCHEMA_OGC_WKT);
        conn.add(VF.createStatement(subject, GeoConstants.GEO_AS_WKT, object));
    }
}
