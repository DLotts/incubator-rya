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
package org.apache.rya.indexing.pcj.fluo.integration;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry;
import org.openrdf.query.impl.BindingImpl;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Performs integration tests over the Fluo application geared towards various query structures.
 * <p>
 * These tests are being ignore so that they will not run as unit tests while building the application.
 */
public class QueryIT extends ITBase {

    @Test
    public void optionalStatements() throws Exception {
        // A query that has optional statement patterns. This query is looking for all
        // people who have Law degrees and any BAR exams they have passed (though they
        // do not have to have passed any).
        final String sparql =
                "SELECT ?person ?exam " +
                "WHERE {" +
                    "?person <http://hasDegreeIn> <http://Law> . " +
                    "OPTIONAL {?person <http://passedExam> ?exam } . " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://hasDegreeIn", "http://Computer Science"),
                makeRyaStatement("http://Alice", "http://passedExam", "http://Certified Ethical Hacker"),
                makeRyaStatement("http://Bob", "http://hasDegreeIn", "http://Law"),
                makeRyaStatement("http://Bob", "http://passedExam", "http://MBE"),
                makeRyaStatement("http://Bob", "http://passedExam", "http://BAR-Kansas"),
                makeRyaStatement("http://Charlie", "http://hasDegreeIn", "http://Law"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add( makeBindingSet(
                new BindingImpl("person", new URIImpl("http://Bob")),
                new BindingImpl("exam", new URIImpl("http://MBE"))));
        expected.add( makeBindingSet(
                new BindingImpl("person", new URIImpl("http://Bob")),
                new BindingImpl("exam", new URIImpl("http://BAR-Kansas"))));
        expected.add( makeBindingSet(
                new BindingImpl("person", new URIImpl("http://Charlie"))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected,  results);
    }

    /**
     * Tests when there are a bunch of variables across a bunch of joins.
     */
    @Test
    public void complexQuery() throws Exception {
        // A query that find people who live in the USA, have been recruited by Geek Squad,
        // and are skilled with computers. The resulting binding set includes everybody who
        // was involved in the recruitment process.
        final String sparql =
                "SELECT ?recruiter ?candidate ?leader " +
                "{ " +
                  "?recruiter <http://recruiterFor> <http://GeekSquad>. " +
                  "?candidate <http://skilledWith> <http://Computers>. " +
                  "?candidate <http://livesIn> \"USA\". " +
                  "?leader <http://leaderOf> <http://GeekSquad>. " +
                  "?recruiter <http://talksTo> ?candidate. " +
                  "?candidate <http://talksTo> ?leader. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                // Leaders
                makeRyaStatement("http://Alice", "http://leaderOf", "http://GeekSquad"),
                makeRyaStatement("http://Bob", "http://leaderOf", "http://GeekSquad"),

                // Recruiters
                makeRyaStatement("http://Charlie", "http://recruiterFor", "http://GeekSquad"),
                makeRyaStatement("http://David", "http://recruiterFor", "http://GeekSquad"),

                // Candidates
                makeRyaStatement("http://Eve", "http://skilledWith", "http://Computers"),
                makeRyaStatement("http://Eve", "http://livesIn", "USA"),
                makeRyaStatement("http://Frank", "http://skilledWith", "http://Computers"),
                makeRyaStatement("http://Frank", "http://livesIn", "USA"),
                makeRyaStatement("http://George", "http://skilledWith", "http://Computers"),
                makeRyaStatement("http://George", "http://livesIn", "Germany"),
                makeRyaStatement("http://Harry", "http://skilledWith", "http://Negotiating"),
                makeRyaStatement("http://Harry", "http://livesIn", "USA"),
                makeRyaStatement("http://Ivan", "http://skilledWith", "http://Computers"),
                makeRyaStatement("http://Ivan", "http://livesIn", "USA"),

                // Candidates the recruiters talk to.
                makeRyaStatement("http://Charlie", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Charlie", "http://talksTo", "http://George"),
                makeRyaStatement("http://Charlie", "http://talksTo", "http://Harry"),
                makeRyaStatement("http://David", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://David", "http://talksTo", "http://Frank"),
                makeRyaStatement("http://David", "http://talksTo", "http://Ivan"),

                // Recruits that talk to leaders.
                makeRyaStatement("http://Eve", "http://talksTo", "http://Alice"),
                makeRyaStatement("http://George", "http://talksTo", "http://Alice"),
                makeRyaStatement("http://Harry", "http://talksTo", "http://Bob"),
                makeRyaStatement("http://Ivan", "http://talksTo", "http://Bob"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add( makeBindingSet(
                new BindingImpl("recruiter", new URIImpl("http://Charlie")),
                new BindingImpl("candidate", new URIImpl("http://Eve")),
                new BindingImpl("leader", new URIImpl("http://Alice"))));
        expected.add( makeBindingSet(
                new BindingImpl("recruiter", new URIImpl("http://David")),
                new BindingImpl("candidate", new URIImpl("http://Eve")),
                new BindingImpl("leader", new URIImpl("http://Alice"))));
        expected.add( makeBindingSet(
                new BindingImpl("recruiter", new URIImpl("http://David")),
                new BindingImpl("candidate", new URIImpl("http://Ivan")),
                new BindingImpl("leader", new URIImpl("http://Bob"))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected,  results);
    }

    @Test
    public void withURIFilters() throws Exception {
        final String sparql =
                "SELECT ?customer ?worker ?city " +
                "{ " +
                  "FILTER(?customer = <http://Alice>) " +
                  "FILTER(?city = <http://London>) " +
                  "?customer <http://talksTo> ?worker. " +
                  "?worker <http://livesIn> ?city. " +
                  "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://talksTo", "http://Bob"),
                makeRyaStatement("http://Bob", "http://livesIn", "http://London"),
                makeRyaStatement("http://Bob", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://Charlie"),
                makeRyaStatement("http://Charlie", "http://livesIn", "http://London"),
                makeRyaStatement("http://Charlie", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://David"),
                makeRyaStatement("http://David", "http://livesIn", "http://London"),
                makeRyaStatement("http://David", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Eve", "http://livesIn", "http://Leeds"),
                makeRyaStatement("http://Eve", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Frank", "http://talksTo", "http://Alice"),
                makeRyaStatement("http://Frank", "http://livesIn", "http://London"),
                makeRyaStatement("http://Frank", "http://worksAt", "http://Chipotle"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Bob")),
                new BindingImpl("city", new URIImpl("http://London"))));
        expected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Charlie")),
                new BindingImpl("city", new URIImpl("http://London"))));
        expected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://David")),
                new BindingImpl("city", new URIImpl("http://London"))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected,  results);
    }

    @Test
    public void withNumericFilters() throws Exception {
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://hasAge", 18),
                makeRyaStatement("http://Bob", "http://hasAge", 30),
                makeRyaStatement("http://Charlie", "http://hasAge", 14),
                makeRyaStatement("http://David", "http://hasAge", 16),
                makeRyaStatement("http://Eve", "http://hasAge", 35),

                makeRyaStatement("http://Alice", "http://playsSport", "Soccer"),
                makeRyaStatement("http://Bob", "http://playsSport", "Soccer"),
                makeRyaStatement("http://Charlie", "http://playsSport", "Basketball"),
                makeRyaStatement("http://Charlie", "http://playsSport", "Soccer"),
                makeRyaStatement("http://David", "http://playsSport", "Basketball"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add( makeBindingSet(
                new BindingImpl("name", new URIImpl("http://Alice")),
                new BindingImpl("age", new NumericLiteralImpl(18, XMLSchema.INTEGER))));
        expected.add( makeBindingSet(
                new BindingImpl("name", new URIImpl("http://Charlie")),
                new BindingImpl("age", new NumericLiteralImpl(14, XMLSchema.INTEGER))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected,  results);
    }
    
    @Test
    public void withCustomFilters() throws Exception {
        final String sparql = "prefix ryafunc: <tag:rya.apache.org,2017:function#> \n" //
                        + "SELECT ?name ?age \n" //
                        + "{ \n" //
                        + "FILTER( ryafunc:isTeen(?age) ) . \n" //
                        + "?name <http://hasAge> ?age . \n" //
                        + "?name <http://playsSport> \"Soccer\" \n" //
                        + "}"; //

        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://hasAge", 18),
                makeRyaStatement("http://Bob", "http://hasAge", 30),
                makeRyaStatement("http://Charlie", "http://hasAge", 14),
                makeRyaStatement("http://David", "http://hasAge", 16),
                makeRyaStatement("http://Eve", "http://hasAge", 35),

                makeRyaStatement("http://Alice", "http://playsSport", "Soccer"),
                makeRyaStatement("http://Bob", "http://playsSport", "Soccer"),
                makeRyaStatement("http://Charlie", "http://playsSport", "Basketball"),
                makeRyaStatement("http://Charlie", "http://playsSport", "Soccer"),
                makeRyaStatement("http://David", "http://playsSport", "Basketball"));

        Function fooFunction = new Function() {

            @Override
            public String getURI() {
                return "tag:rya.apache.org,2017:function#isTeen";
            }

            final static int TEEN_THRESHOLD = 20;

            @Override
            public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {

                if (args.length != 1) {
                    throw new ValueExprEvaluationException("isTeen() requires exactly 1 argument, got " + args.length);
                }

                if (args[0] instanceof Literal) {
                    Literal literal = (Literal) args[0];

                    URI datatype = literal.getDatatype();

                    // ABS function accepts only numeric literals
                    if (datatype != null && XMLDatatypeUtil.isNumericDatatype(datatype)) {
                        if (XMLDatatypeUtil.isDecimalDatatype(datatype)) {
                            BigDecimal bigValue = literal.decimalValue();
                            return BooleanLiteralImpl.valueOf(bigValue.compareTo(new BigDecimal(TEEN_THRESHOLD)) < 0);
                        } else if (XMLDatatypeUtil.isFloatingPointDatatype(datatype)) {
                            double doubleValue = literal.doubleValue();
                            return BooleanLiteralImpl.valueOf(doubleValue < TEEN_THRESHOLD);
                        } else {
                            throw new ValueExprEvaluationException("unexpected datatype (expect decimal/int or floating) for function operand: " + args[0]);
                        }
                    } else {
                        throw new ValueExprEvaluationException("unexpected input value (expect non-null and numeric) for function: " + args[0]);
                    }
                } else {
                    throw new ValueExprEvaluationException("unexpected input value (expect literal) for function: " + args[0]);
                }
            }
        };

        // Add our new function to the registry
        FunctionRegistry.getInstance().add(fooFunction);

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add( makeBindingSet(
                new BindingImpl("name", new URIImpl("http://Alice")),
                new BindingImpl("age", new NumericLiteralImpl(18, XMLSchema.INTEGER))));
        expected.add( makeBindingSet(
                new BindingImpl("name", new URIImpl("http://Charlie")),
                new BindingImpl("age", new NumericLiteralImpl(14, XMLSchema.INTEGER))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected,  results);
    }

    @Test
    public void withGeoFilters() throws Exception {
        final static String geoWithinSelect = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "//
                        + "PREFIX ryageo: <tag:rya.apache.org,2017:function/geo#> "//
                        + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "//
                        + "SELECT ?feature ?point ?wkt " //
                        + "{" //
                        + " ?feature a geo:Feature . "//
                        + " ?feature geo:hasGeometry ?point . "//
                        + " ?point a geo:Point . "//
                        + " ?point geo:asWKT ?wkt . "//
                        + " FILTER(ryageo:ehContains(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) " //
                        + "}";//
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(makeRyaStatement("tag:rya.apache.org,2017:ex#feature", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.opengis.net/ont/geosparql#Feature"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#feature", "http://www.opengis.net/ont/geosparql#hasGeometry", "tag:rya.apache.org,2017:ex#test_point"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#test_point", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.opengis.net/ont/geosparql#Point"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#test_point", "http://www.opengis.net/ont/geosparql#asWKT", "Point(-77.03524 38.889468)\"^^http://www.opengis.net/ont/geosparql#wktLiteral\"") //
        );

        Function fooFunction = new Function() {

            @Override
            public String getURI() {
                return "tag:rya.apache.org,2017:function/geo#ehContains";
            }

            @Override
            public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {

                if (args.length != 2) {
                    throw new ValueExprEvaluationException(getURI() + " requires exactly 3 arguments, got " + args.length);
                }
                SpatialContext spatialContext =  .getSpatialContext();
                Shape shape1 = FunctionArguments.getShape(this, args[0], spatialContext);
                Shape shape2 = FunctionArguments.getShape(this, args[1], spatialContext);
                //https://github.com/eclipse/rdf4j/blob/master/core/queryalgebra/geosparql/src/main/java/org/eclipse/rdf4j/query/algebra/evaluation/function/geosparql/SpatialSupport.java
                boolean result = SpatialSupport.getSpatialAlgebra().ehContains(shape1, shape2);
                return valueFactory.createLiteral(result);
            }
        };

        // Add our new function to the registry
        FunctionRegistry.getInstance().add(fooFunction);

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("name", new URIImpl("http://Alice")), new BindingImpl("age", new NumericLiteralImpl(18, XMLSchema.INTEGER))));
        expected.add(makeBindingSet(new BindingImpl("name", new URIImpl("http://Charlie")), new BindingImpl("age", new NumericLiteralImpl(14, XMLSchema.INTEGER))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(geoWithinSelect);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String> absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, geoWithinSelect);
        assertEquals(expected, results);
    }

    final static String geoDataInsert = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
    +"INSERT DATA { " //
    +"  <urn:feature> a geo:Feature ; " //
    +"    geo:hasGeometry [ " //
    +"      a geo:Point ; " //
    +"      geo:asWKT \"Point(-77.03524 38.889468)\"^^geo:wktLiteral "//
    +"    ] . " //
    +"}";

}

// private static void testAddPointAndWithinSearchWithPCJ(
// final SailRepositoryConnection conn) throws Exception {
//
// final String update = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "//
// + "INSERT DATA { " //
// + " <urn:feature> a geo:Feature ; " //
// + " geo:hasGeometry [ " //
// + " a geo:Point ; " //
// + " geo:asWKT \"Point(-77.03524 38.889468)\"^^geo:wktLiteral "//
// + " ] . " //
// + "}";
//
// final Update u = conn.prepareUpdate(QueryLanguage.SPARQL, update);
// u.execute();
//
// String queryString;
// TupleQuery tupleQuery;
// CountingResultHandler tupleHandler;
//
//// point outside search ring
// queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "//
// + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "//
// + "SELECT ?feature ?point ?wkt " //
// + "{" //
// + " ?feature a geo:Feature . "//
// + " ?feature geo:hasGeometry ?point . "//
// + " ?point a geo:Point . "//
// + " ?point geo:asWKT ?wkt . "//
// + " FILTER(geof:sfWithin(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) " //
// + "}";//
// tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
// tupleHandler = new CountingResultHandler();
// tupleQuery.evaluate(tupleHandler);
// log.info("Result count : " + tupleHandler.getCount());
// Validate.isTrue(tupleHandler.getCount() == 0);
//
//// point inside search ring
// queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "//
// + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "//
// + "SELECT ?feature ?point ?wkt ?e ?l ?o" //
// + "{" //
// + " ?feature a ?e . "//
// + " ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
// + " ?e <uri:talksTo> ?o . "//
// + " ?feature a geo:Feature . "//
// + " ?feature geo:hasGeometry ?point . "//
// + " ?point a geo:Point . "//
// + " ?point geo:asWKT ?wkt . "//
// + " FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
// + "}";//
//
// tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
// tupleHandler = new CountingResultHandler();
// tupleQuery.evaluate(tupleHandler);
// log.info("Result count : " + tupleHandler.getCount());
// Validate.isTrue(tupleHandler.getCount() == 1);
//
//// point inside search ring with Pre-Computed Join
// queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "//
// + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "//
// + "SELECT ?feature ?point ?wkt ?e ?l ?o" //
// + "{" //
// + " ?feature a ?e . "//
// + " ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
// + " ?e <uri:talksTo> ?o . "//
// + " ?feature a geo:Feature . "//
// + " ?feature geo:hasGeometry ?point . "//
// + " ?point a geo:Point . "//
// + " ?point geo:asWKT ?wkt . "//
// + " FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
// + "}";//
//
// tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
// tupleHandler = new CountingResultHandler();
// tupleQuery.evaluate(tupleHandler);
// log.info("Result count : " + tupleHandler.getCount());
// Validate.isTrue(tupleHandler.getCount() >= 1); // may see points from
// // during previous runs
//
//// point outside search ring with PCJ
// queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "//
// + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "//
// + "SELECT ?feature ?point ?wkt ?e ?l ?o " //
// + "{" //
// + " ?feature a ?e . "//
// + " ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
// + " ?e <uri:talksTo> ?o . "//
// + " ?feature a geo:Feature . "//
// + " ?feature geo:hasGeometry ?point . "//
// + " ?point a geo:Point . "//
// + " ?point geo:asWKT ?wkt . "//
// + " FILTER(geof:sfWithin(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) " //
// + "}";//
// tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
// tupleHandler = new CountingResultHandler();
// tupleQuery.evaluate(tupleHandler);
// log.info("Result count : " + tupleHandler.getCount());
// Validate.isTrue(tupleHandler.getCount() == 0);
//
//// point inside search ring with different Pre-Computed Join
// queryString = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "//
// + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "//
// + "SELECT ?feature ?point ?wkt ?e ?c ?l ?o " //
// + "{" //
// + " ?e a ?c . "//
// + " ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
// + " ?e <uri:talksTo> ?o . "//
// + " ?feature a geo:Feature . "//
// + " ?feature geo:hasGeometry ?point . "//
// + " ?point a geo:Point . "//
// + " ?point geo:asWKT ?wkt . "//
// + " FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -77 39, -77 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
// + "}";//
// tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
// tupleHandler = new CountingResultHandler();
// tupleQuery.evaluate(tupleHandler);
// log.info("Result count : " + tupleHandler.getCount());
// Validate.isTrue(tupleHandler.getCount() == 1);
// }
//
