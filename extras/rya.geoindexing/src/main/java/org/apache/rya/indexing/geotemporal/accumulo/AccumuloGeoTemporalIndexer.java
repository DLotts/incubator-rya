/**
 * 
 */
package org.apache.rya.indexing.geotemporal.accumulo;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.experimental.AbstractAccumuloIndexer;
import org.apache.rya.accumulo.experimental.AccumuloIndexer;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.GeoIndexer;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexer;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException;
import org.geotools.feature.DefaultFeatureCollection;
import org.opengis.feature.simple.SimpleFeature;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import info.aduna.iteration.CloseableIteration;

/**
 * Use GeoMesa to do date+time indexing along with geospatial
 *
 */
public class AccumuloGeoTemporalIndexer extends AbstractAccumuloIndexer implements AccumuloIndexer, GeoTemporalIndexer, GeoIndexer {
	private static final Logger LOG = Logger.getLogger(AccumuloGeoTemporalIndexer.class);
    // Tables will be grouped by this name (table prefix)
    private String ryaInstanceName;
    private AccumuloEventStorage eventStorage;
    private Configuration conf;
	private Connector connector = null;
    private Set<URI> validPredicates;

    /**
     * 
     */
//    public AccumuloGeoTemporalIndexer(final Connector accumulo, final String ryaInstanceName) {
//        this.ryaInstanceName = requireNonNull(ryaInstanceName);
//    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#getTableName()
     */
    @Override
    public String getTableName() {
        return ryaInstanceName + "_geoTemporal";
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#storeStatements(java.util.Collection)
     */
    @Override
    public void storeStatements(Collection<RyaStatement> statements) throws IOException {
        // TODO this is just a rough version
        // create a feature collection
        for (final RyaStatement ryaStatement : statements) {
//            final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
            // if the predicate list is empty, accept all predicates.
            // Otherwise, make sure the predicate is on the "valid" list
            final boolean isValidPredicate = validPredicates==null || validPredicates.isEmpty() || validPredicates.contains(ryaStatement.getPredicate());

            if (isValidPredicate && (ryaStatement.getObject() instanceof Literal)) {
                try {
                	// convert from statement.getObject() WKT/GML to JTS geometry
                    final Geometry geo = convertWktGmlToGeometry(ryaStatement.getObject());
        			TemporalInstant tempo = null; //TODO convert from statement.getObject() literal to temporalInstant
    				final Event event = Event.builder() //
        					.setSubject(ryaStatement.getSubject()) //
        					.setGeometry(geo) //TODO pick geo or temporal
        					.setTemporalInstant(tempo) //
        					.build(); 
        			try {
    					eventStorage.create(event);
    				} catch (ObjectStorageException e) {
    					throw new IOException("Trouble while writing a new statement to the geotemporal index.", e);
    				}
                } catch (final /*Parse*/Exception e) {
                    LOG.warn("Error getting geo from statement: " + ryaStatement.toString(), e);
                }
            }
        }
    }

    private Geometry convertWktGmlToGeometry(RyaType object) {
		// TODO Auto-generated method stub
		return null;
	}

	private SimpleFeature createFeature(Object featureType, RyaStatement ryaStatement) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#storeStatement(org.apache.rya.api.domain.RyaStatement)
     */
    @Override
    public void storeStatement(RyaStatement statement) throws IOException {
        storeStatements(Collections.singleton(statement));
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#deleteStatement(org.apache.rya.api.domain.RyaStatement)
     */
    @Override
    public void deleteStatement(RyaStatement stmt) throws IOException {
        // TODO Auto-generated method stub
    throw new Error("Not yet implemented.");
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#dropGraph(org.apache.rya.api.domain.RyaURI[])
     */
    @Override
    public void dropGraph(RyaURI... graphs) {
        // TODO Auto-generated method stub
    throw new Error("Not yet implemented.");
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#getIndexablePredicates()
     */
    @Override
    public Set<URI> getIndexablePredicates() {
        // TODO Auto-generated method stub
    throw new Error("Not yet implemented.");
        //return null;
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#flush()
     */
    @Override
    public void flush() throws IOException {
        // TODO Auto-generated method stub
    throw new Error("Not yet implemented.");
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#close()
     */
    @Override
    public void close() throws IOException {
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public void setConf(Configuration conf) {
        Objects.requireNonNull("Required parameter: conf");
        this.conf = conf;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /* (non-Javadoc)
     * @see org.apache.rya.indexing.GeoTemporalIndexer#getEventStorage(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public EventStorage getEventStorage(Configuration conf) {
        Objects.requireNonNull(eventStorage, "This is not initialized, call setconf() and init() first.");
        return eventStorage;
    }

    @Override
    public void init() {
        if (eventStorage==null){
            eventStorage = new AccumuloEventStorage();
            eventStorage.init(conf);
        }
    }

    @Override
    public void setConnector(Connector connector) {
    	this.connector  = connector;
    }

    @Override
    public void destroy() {
        // do nothing
    }

    @Override
    public void purge(RdfCloudTripleStoreConfiguration configuration) {
        // TODO Auto-generated method stub
        throw new Error("Not yet implemented.");
    }

    @Override
    public void dropAndDestroy() {
        // TODO Auto-generated method stub
        throw new Error("Not yet implemented.");
    }

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> queryEquals(Geometry query,
			StatementConstraints contraints) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> queryDisjoint(Geometry query,
			StatementConstraints contraints) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> queryIntersects(Geometry query,
			StatementConstraints contraints) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> queryTouches(Geometry query,
			StatementConstraints contraints) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> queryCrosses(Geometry query,
			StatementConstraints contraints) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> queryWithin(Geometry query,
			StatementConstraints contraints) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> queryContains(Geometry query,
			StatementConstraints contraints) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> queryOverlaps(Geometry query,
			StatementConstraints contraints) {
		// TODO Auto-generated method stub
		return null;
	}
}
