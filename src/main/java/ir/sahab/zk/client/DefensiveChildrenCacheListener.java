package ir.sahab.zk.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It is a wrapper around the default {@link PathChildrenCacheListener}. In normal situations, there is no difference
 * between this and the default listener. The difference occurs when processing a child event, throws an exception. If
 * an exception occurs (even Runtime exceptions) by the default listener, just a log will
 * be added, but by this listener, it causes crash.
 */
public abstract class DefensiveChildrenCacheListener implements PathChildrenCacheListener {

    private static final Logger logger = LoggerFactory.getLogger(DefensiveChildrenCacheListener.class);

    @SuppressWarnings("java:S1181")
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        try {
            onChildEvent(client, event);
        } catch (Throwable t) {
            logger.error("Crashed because of: ", t);
            System.exit(1);
        }
    }

    public abstract void onChildEvent(CuratorFramework client, PathChildrenCacheEvent event);
}