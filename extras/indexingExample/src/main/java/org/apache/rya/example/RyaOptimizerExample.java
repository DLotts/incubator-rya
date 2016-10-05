package org.apache.rya.example;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.geo.GeoConstants;
import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig;
import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.RdfCloudTripleStoreConnection;
import mvm.rya.sail.config.RyaSailFactory;

public class RyaOptimizerExample {
    private static final Logger log = Logger.getLogger(RyaOptimizerExample.class);

    //
    // Connection configuration parameters
    //

    private static final boolean USE_MOCK_INSTANCE = true;
    private static final boolean PRINT_QUERIES = true;
    private static final String INSTANCE = "instance";
    private static final String RYA_TABLE_PREFIX = "x_test_triplestore_";
    private static final String AUTHS = "U";

    public static void main(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, PRINT_QUERIES);

        SailRepository repository = null;
        SailRepositoryConnection conn = null;

        try {
            log.info("Connecting to Indexing Sail Repository.");
            final Sail extSail = RyaSailFactory.getInstance(conf);
            repository = new SailRepository(extSail);
            conn = repository.getConnection();

            final long start = System.currentTimeMillis();
            log.info("Running SPARQL Example: Add and Delete");
            testAddAndDelete((RdfCloudTripleStore) extSail, conn);

            log.info("TIME: " + (System.currentTimeMillis() - start) / 1000.);
        } finally {
            log.info("Shutting down");
            closeQuietly(conn);
            closeQuietly(repository);
        }
    }

    private static void closeQuietly(final SailRepository repository) {
        if (repository != null) {
            try {
                repository.shutDown();
            } catch (final RepositoryException e) {
                // quietly absorb this exception
            }
        }
    }

    private static void closeQuietly(final SailRepositoryConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (final RepositoryException e) {
                // quietly absorb this exception
            }
        }
    }

    private static Configuration getConf() {

        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, USE_MOCK_INSTANCE);
        conf.setBoolean(ConfigUtils.USE_PCJ, false);
        conf.setBoolean(ConfigUtils.USE_GEO, true);
        conf.setBoolean(ConfigUtils.USE_FREETEXT, true);
        conf.setBoolean(ConfigUtils.USE_TEMPORAL, true);

        conf.set(PrecomputedJoinIndexerConfig.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.ACCUMULO.name());
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, RYA_TABLE_PREFIX);
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, INSTANCE);
        conf.setInt(ConfigUtils.NUM_PARTITIONS, 3);
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, AUTHS);

        // only geo index statements with geo:asWKT predicates
        conf.set(ConfigUtils.GEO_PREDICATES_LIST, GeoConstants.GEO_AS_WKT.stringValue());
        return conf;
    }

    public static void testAddAndDelete(RdfCloudTripleStore store, final SailRepositoryConnection conn) throws MalformedQueryException, RepositoryException, UpdateExecutionException, QueryEvaluationException, TupleQueryResultHandlerException, AccumuloException, AccumuloSecurityException, TableNotFoundException, SailException {
        final String insert = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " + "PREFIX time: <http://www.w3.org/2006/time#> " + "INSERT DATA { " + "_:x a <t:event> ; " + "      <t:hasName> 'event'; " + "      <t:startDate> [ " + "        a time:Instant; " + "        time:inXSDDateTime '2016-8-25T12:00:00Z' ; " + "      ]; " + "      <t:endDate> [ " + "        a time:Instant; " + "        time:inXSDDateTime '2016-8-25T13:00:00Z' ; " + "      ]; " + "      geo:hasGeometry  [ " + "        geo:asWKT 'POINT (1 1)'^^geo:wktLiteral ; " + "      ]. " + "}";

        final Update update = conn.prepareUpdate(QueryLanguage.SPARQL, insert);
        // insert three events
        update.execute();
        update.execute();
        update.execute();

        final String query = "PREFIX time: <http://www.w3.org/2006/time#> " + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> " + "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " + "SELECT ?event ?startTime ?endTime ?wkt " + "WHERE { " +

                        "  ?location geo:asWKT ?wkt . " + "    FILTER(geof:sfWithin(?wkt, 'POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'^^geo:wktLiteral)) . " +

                        "  ?event geo:hasGeometry ?location . " +

                        "  ?event <t:startDate> ?start . " + "  ?start time:inXSDDateTime ?startTime . " + "    FILTER(tempo:before(?startTime, '2017-8-25T12:00:00Z') ) . " +
                        // " FILTER(?startTime < '2017-8-25T12:00:00+0500') " +

                        "  ?event <t:endDate> ?end . " + "  ?end time:inXSDDateTime ?endTime . " + "    FILTER(tempo:after(?endTime, '2015-8-25T12:00:00Z') ) . " +
                        // " FILTER(?endTime > '2015-8-25T12:00:00-0200') " +
                        " }";

        // The usual evaluation:
        final CountingResultHandler resultHandler = new CountingResultHandler();
        TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
        tupleQuery.evaluate(resultHandler);
        log.info("Result count : " + resultHandler.getCount());
        Validate.isTrue(resultHandler.getCount() == 3);
        resultHandler.resetCount();

        RdfCloudTripleStoreConnection.reportCartProduct(((SailTupleQuery) tupleQuery).getParsedQuery().getTupleExpr(), "after evaluation");

        // // Instead of the usual evaluation, lets try different optimizers:
        // final SPARQLParser parser = new SPARQLParser();
        // final ParsedQuery pq = parser.parseQuery(query, null);
        // final TupleExpr te = pq.getTupleExpr();
        //
        // // QueryModelNormalizer optimizerQuery = new QueryModelNormalizer();
        // // optimizerQuery.optimize(te, null, null);
        // EvaluationStatistics stats = new DefaultStatistics();
        // (new mvm.rya.rdftriplestore.evaluation.QueryJoinOptimizer(stats)).optimize(te, null, null);
        //
        // RdfCloudTripleStoreConnection.reportCartProduct(te, "after QueryJoinOptimizer");
        //
        // FilterFunctionOptimizer optimizerFilterFunction = new FilterFunctionOptimizer();
        // optimizerFilterFunction.setConf(getConf());
        // optimizerFilterFunction.optimize(te, null, null);
        //
        // RdfCloudTripleStoreConnection.reportCartProduct(te, "before QueryUnCartesianProductOptimizer");
        //
        // QueryUnCartesianProductOptimizer optimizerUnCart = new QueryUnCartesianProductOptimizer();
        //
        // optimizerUnCart.optimize(te, null, null);
        // RdfCloudTripleStoreConnection.reportCartProduct(te, "After QueryUnCartesianProductOptimizer");

        //// // Now evaluate, but the evaluate method will run all the optimizers.
        //// CloseableIteration<? extends BindingSet, QueryEvaluationException> iterator = (store.getConnection()).evaluate(te, null, null, true);
        //// while(iterator.hasNext()) {
        //// System.out.println("result optimized: "+iterator.next());
        //// }

    }

    private static class CountingResultHandler implements TupleQueryResultHandler {
        private int count = 0;

        public int getCount() {
            return count;
        }

        public void resetCount() {
            count = 0;
        }

        @Override
        public void startQueryResult(final List<String> arg0) throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(final BindingSet arg0) throws TupleQueryResultHandlerException {
            count++;
            System.out.println(arg0);
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleBoolean(final boolean arg0) throws QueryResultHandlerException {
        }

        @Override
        public void handleLinks(final List<String> arg0) throws QueryResultHandlerException {
        }
    }
}
