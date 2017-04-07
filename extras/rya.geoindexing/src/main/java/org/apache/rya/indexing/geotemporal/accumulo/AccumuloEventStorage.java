/**
 * 
 */
package org.apache.rya.indexing.geotemporal.accumulo;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Optional;

import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;

/**
 * 
 *
 */
public class AccumuloEventStorage implements EventStorage {
    private static final Logger LOG = Logger.getLogger(AccumuloGeoTemporalIndexer.class);
    // Store/retrieve on this Accumulo cluster
    private Connector accumulo;
    // Tables will be grouped by this name (table prefix)
    private String ryaInstanceName;

    /**
     * Construct with a working accumulo and rya name
     */
    public AccumuloEventStorage(final Connector accumulo, final String ryaInstanceName) {
        this.accumulo = requireNonNull(accumulo);
        this.ryaInstanceName = requireNonNull(ryaInstanceName);
        // TODO We don't have a strategy yet
        // queryAdapter = new GeoTemporalAccumuloDBStorageStrategy();
    }

    /* (non-Javadoc)
     * @see org.apache.rya.indexing.mongodb.update.RyaObjectStorage#create(java.lang.Object)
     */
    @Override
    public void create(Event doc) throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectAlreadyExistsException, org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.rya.indexing.mongodb.update.RyaObjectStorage#get(org.apache.rya.api.domain.RyaURI)
     */
    @Override
    public Optional<Event> get(RyaURI subject) throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.rya.indexing.mongodb.update.RyaObjectStorage#update(java.lang.Object, java.lang.Object)
     */
    @Override
    public void update(Event old, Event updated) throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.StaleUpdateException, org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.apache.rya.indexing.mongodb.update.RyaObjectStorage#delete(org.apache.rya.api.domain.RyaURI)
     */
    @Override
    public boolean delete(RyaURI subject) throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Collection<Event> search(Optional<RyaURI> subject, Optional<Collection<IndexingExpr>> geoFilters, Optional<Collection<IndexingExpr>> temporalFilters) throws org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException {
        // TODO Auto-generated method stub
        return null;
    }


}
