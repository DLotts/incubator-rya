/**
 * 
 */
package org.apache.rya.indexing.geotemporal.accumulo;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexer;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.openrdf.model.URI;

/**
 * Use GeoMesa to do date+time indexing along with geospatial
 *
 */
public class AccumuloGeoTemporalIndexer implements GeoTemporalIndexer {
    // Tables will be grouped by this name (table prefix)
    private String ryaInstanceName;
    /**
     * 
     */
    public AccumuloGeoTemporalIndexer(final Connector accumulo, final String ryaInstanceName) {
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
    }

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
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#storeStatement(org.apache.rya.api.domain.RyaStatement)
     */
    @Override
    public void storeStatement(RyaStatement statement) throws IOException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#deleteStatement(org.apache.rya.api.domain.RyaStatement)
     */
    @Override
    public void deleteStatement(RyaStatement stmt) throws IOException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#dropGraph(org.apache.rya.api.domain.RyaURI[])
     */
    @Override
    public void dropGraph(RyaURI... graphs) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#getIndexablePredicates()
     */
    @Override
    public Set<URI> getIndexablePredicates() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#flush()
     */
    @Override
    public void flush() throws IOException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.rya.api.persist.index.RyaSecondaryIndexer#close()
     */
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public void setConf(Configuration conf) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.conf.Configurable#getConf()
     */
    @Override
    public Configuration getConf() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.rya.indexing.GeoTemporalIndexer#getEventStorage(org.apache.hadoop.conf.Configuration)
     */
    @Override
    public EventStorage getEventStorage(Configuration conf) {
        // TODO Auto-generated method stub
        return null;
    }
}
