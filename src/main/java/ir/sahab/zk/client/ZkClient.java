package ir.sahab.zk.client;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thin wrapper around Apache Curator. Compared with Curator, we will see the following differences:
 * <ul>
 * <li><b>Checked Exceptions</b>:
 * It prefers checked exceptions over runtime ones to avoid exceptions being missed or unhandled.
 * So we see {@link InterruptedException} and {@link ZkClientException} in signature of the operation calls.
 * {@link ZkClientException} is a checked exception that is a wrapper around the {@link KeeperException}.
 * It provides two useful methods:
 * <ul>
 * <li>{@link ZkClientException#getZkErrorCode()}: returns the standard ZK error code extracted from the underlying
 * `KeeperException`.</li>
 * <li>{@link ZkClientException#canResolveByRetry()}: Some network related exceptions can be fixed by retry, but error
 * codes like {@link Code#NONODE} or {@link Code#NODEEXISTS} should be handled differently. This method helps to
 * separate these two categories from each other.</li>
 * </ul>
 * </li>
 * <li><b>Concise Operation Calls:</b>
 * The operations are written in a more concise manner. For example the equivalent of curator call
 * `curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);` is
 * `zkClient.addEphemeralNode(path, data);`. However, it is possible to go back to the curator style, by getting the
 * underlying curator object from `ZkClient` instance:
 * {@link ZkClient#getUnderlyingCurator()}, and this way you have access to the full feature list of curator.
 * </li>
 * </ul>
 *
 * <p>Note: All methods of this class are thread safe except {@link #start(String, int, int, int, int)} and
 * {@link #close()} which should be called once per instance of the class. All interactions of to ZooKeeper server can
 * be done through this class.
 * </p>
 *
 * @see <a href="https://curator.apache.org/">Apache Curator</a>
 */
public class ZkClient implements ConnectionStateListener, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ZkClient.class);
    private static final int MAX_CURATOR_TIMEOUT_SECONDS = 30;

    private CuratorFramework curator = null;
    private Consumer<ConnectionState> connectionStateChangeCallback = null;

    /**
     * Connects to ZK servers and blocks until connection is established. It retries forever (has no timeout).
     *
     * @param serverAddresses Addresses of ZK servers in comma separated IP:port format.
     * @param connectionTimeoutMillis Timeout for connecting to ZK servers.
     * @param sessionTimeoutMillis Timeout that if we have not access to the servers, the session would be lost.
     * @param numRetriesForFailedZkOps Number of retries which should be automatically done before raising an exception
     *     on a operation.
     * @param sleepBetweenRetriesMillis Sleep time between retries for failed operations.
     * @throws InterruptedException If interrupted before connection established.
     */
    public void start(String serverAddresses, int connectionTimeoutMillis, int sessionTimeoutMillis,
            int numRetriesForFailedZkOps, int sleepBetweenRetriesMillis) throws InterruptedException {
        RetryPolicy retryPolicy = new RetryNTimes(numRetriesForFailedZkOps,
                sleepBetweenRetriesMillis);
        curator = CuratorFrameworkFactory.newClient(serverAddresses, sessionTimeoutMillis,
                connectionTimeoutMillis, retryPolicy);
        curator.getConnectionStateListenable().addListener(this);

        curator.start();
        curator.blockUntilConnected(MAX_CURATOR_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public void start(String serverAddresses) throws InterruptedException {
        start(serverAddresses, 2000, 2000, 1, 100);
    }

    public void close() {
        CloseableUtils.closeQuietly(curator);
    }

    public CuratorFramework getUnderlyingCurator() {
        return curator;
    }

    /**
     * Gets data of the ZK node with given path.
     *
     * @param path path of the ZK node
     * @throws ZkClientException If can not get data from the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public byte[] getData(String path) throws ZkClientException, InterruptedException {
        try {
            byte[] data = curator.getData().forPath(path);
            return data != null ? data : new byte[0];
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ZkClientException("Failed to get data of the ZK node in path: " + path, ex);
        }
    }

    /**
     * Gets data of the ZK node with given path as string.
     *
     * @param path path of the ZK node
     * @throws ZkClientException If can not get data from the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public String getDataAsString(String path) throws ZkClientException, InterruptedException {
        return new String(getData(path));
    }

    /**
     * Gets data of the ZK node with given path as integer.
     *
     * @param path path of theZk ZK node
     * @throws ZkClientException If can not get data from the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public int getDataAsInteger(String path) throws ZkClientException, InterruptedException {
        return Integer.parseInt(getDataAsString(path));
    }

    /**
     * Gets data of the ZK node with given path as long.
     *
     * @param path path of the ZK node
     * @throws ZkClientException If can not get data from the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public long getDataAsLong(String path) throws ZkClientException, InterruptedException {
        return Long.parseLong(getDataAsString(path));
    }

    /**
     * Gets data of the ZK node with given path as boolean.
     *
     * @param path path of the ZK node
     * @throws ZkClientException If can not get data from the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public boolean getDataAsBoolean(String path) throws ZkClientException, InterruptedException {
        return Boolean.parseBoolean(getDataAsString(path));
    }

    /**
     * Traverses a subtree of ZK and returns a list of visited paths.
     */
    public List<String> getSubPaths(String path) throws ZkClientException, InterruptedException {
        try {
            return ZKUtil.listSubTreeBFS(curator.getZookeeperClient().getZooKeeper(), path);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ZkClientException("Failed to list the subPaths of ZK path: " + path, ex);
        }
    }

    /**
     * Returns ZK node names of the children of a ZK node with the given path.
     *
     * @param path to the parent ZK node
     * @return ZK node names of the children
     * @throws ZkClientException If can not get children of the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public List<String> getChildren(String path) throws ZkClientException, InterruptedException {
        try {
            return curator.getChildren().forPath(path);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ZkClientException("Failed to list the children of ZK path: " + path, ex);
        }
    }

    /**
     * Checks if there is a ZK node in the given path.
     *
     * @param path of the ZK node
     * @return true if there is a ZK node on the path
     * @throws ZkClientException If can not get stats of the path and the error condition is not retry-able or all
     *     retries failed.
     */
    public boolean exists(String path) throws ZkClientException, InterruptedException {
        return getStat(path) != null;
    }

    /**
     * Returns metadata about the ZK node in the given path and null if there is no ZK node in the given path.
     *
     * @param path of the ZK node
     * @return stat metadata
     * @throws ZkClientException If can not get stats of the path and the error condition is not retry-able or all
     *     retries failed.
     */
    public Stat getStat(String path) throws ZkClientException, InterruptedException {
        try {
            return curator.checkExists().forPath(path);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ZkClientException("Failed to get stats of ZK path: " + path, ex);
        }
    }

    /**
     * Returns number of the children of a ZK node with the given path.
     *
     * @param path of the ZK node
     * @return number of the children of a ZK node with the given path
     * @throws ZkClientException If can not get stats of the path and the error condition is not retry-able or all
     *     retries failed.
     */
    public int getNumChildren(String path) throws ZkClientException, InterruptedException {
        Stat stat = getStat(path);
        if (stat != null) {
            return stat.getNumChildren();
        } else {
            throw new ZkClientException("Failed to get number of children of ZK path: " + path
                    + ". The path does not exist", new KeeperException.NoNodeException(path));
        }
    }

    /**
     * Adds an ephemeral ZK node with the specified data to the given path.
     *
     * @param path path of the ZK node that should be created
     * @param data data of the ZK node. It can be null.
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public void addEphemeralNode(String path, byte[] data) throws ZkClientException, InterruptedException {
        try {
            curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);
            if (logger.isDebugEnabled()) {
                logger.debug("Ephemeral ZK node {} added to ZK.", path);
            }
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ZkClientException("Failed to add an ephemeral ZK node in path: " + path, ex);
        }
    }

    /**
     * Adds a sequential ZK node with the specified data and mode to the given path.
     *
     * @param parentPath path of the parent ZK node in which the ZK node should be created
     * @param namePrefix the prefix of the ZK node name
     * @param data data of the ZK node. It can be null.
     * @return the actual ZK node name that is created. The actual ZK node name is the given ZK node prefix plus a
     *     suffix "i" where i is the sequential number.
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public String addSequentiallyNamedNode(String parentPath, String namePrefix, byte[] data)
            throws ZkClientException, InterruptedException {
        String pathPrefix = "";
        try {
            pathPrefix = joinPaths(parentPath, namePrefix);
            String path = curator.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(pathPrefix, data);
            if (logger.isDebugEnabled()) {
                logger.debug("Sequentially named node {} added to ZK.", path);
            }
            return ZKPaths.getNodeFromPath(path);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ZkClientException("Failed to add a sequentially named child on: " + pathPrefix, ex);
        }
    }

    /**
     * Adds a persistent ZK node with the specified data in the given path.
     *
     * @param path path of the ZK node that should be created
     * @param data data of the ZK node. It can be null.
     * @param createParent if true then missing parent dirs are created, if false just create current node.
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public void addPersistentNode(String path, byte[] data, boolean createParent)
            throws ZkClientException, InterruptedException {
        // There is a corner case where Curator throws a ConnectionLoss but actually we added
        // the ZK node. In order to be sure that the logic used for the exceptional case is correct,
        // it is necessary to first check the existence of the path and if it exists, skip the rest
        // of the operations.
        if (exists(path)) {
            throw new ZkClientException("Failed to add ZK node because node exists: path = " + path);
        }

        try {
            if (createParent) {
                curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data);
            } else {
                curator.create().withMode(CreateMode.PERSISTENT).forPath(path, data);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Persistent node {} added to ZK.", path);
            }
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            // There is a corner case where Curator throws a ConnectionLoss but actually we added
            // the ZK node, so when we retry we get NodeExists exception. In here, we check whether
            // node created or not and ignore error if exists.
            if (exists(path) && Arrays.equals(getData(path), data)) {
                logger.warn("Retrieve error in creating ZK path but node created anyway: path = {}", path);
            } else {
                throw new ZkClientException("Failed to add a persistent ZK node: path = {}" + path);
            }
        }
    }

    /**
     * Adds a persistent ZK node with the specified data in the given path.
     *
     * @param path path of the ZK node that should be created
     * @param data data of the ZK node. It can be null.
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public void addPersistentNode(String path, byte[] data) throws ZkClientException, InterruptedException {
        addPersistentNode(path, data, false);
    }

    /**
     * Adds a persistent ZK node with the specified data in the given path.
     *
     * @param path path of the ZK node that should be created
     * @param data data of the ZK node. It can be null.
     * @param createParents if true then missing parent dirs are created, if false just create current node.
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public void addPersistentNode(String path, String data, boolean createParents)
            throws ZkClientException, InterruptedException {
        addPersistentNode(path, data == null ? null : data.getBytes(), createParents);
    }

    /**
     * Adds a persistent ZK node with the specified data in the given path.
     *
     * @param path path of the ZK node that should be created
     * @param data data of the ZK node. It can be null.
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public void addPersistentNode(String path, String data) throws ZkClientException, InterruptedException {
        addPersistentNode(path, data, false);
    }

    /**
     * Adds a persistent ZK node in the given path without any value.
     *
     * @param path path of the ZK node that should be created
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public void addPersistentNode(String path) throws ZkClientException, InterruptedException {
        addPersistentNode(path, "");
    }

    /**
     * Adds a persistent ZK node with the specified integer value in the given path.
     *
     * @param path path of the ZK node that should be created
     * @param data an integer value to put in the created ZK node.
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public void addPersistentNode(String path, int data) throws ZkClientException, InterruptedException {
        addPersistentNode(path, String.valueOf(data), false);
    }

    /**
     * Adds a persistent ZK node with the specified long value in the given path.
     *
     * @param path path of the ZK node that should be created
     * @param data a long value to put in the created ZK node.
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public void addPersistentNode(String path, long data) throws ZkClientException, InterruptedException {
        addPersistentNode(path, String.valueOf(data), false);
    }

    /**
     * Adds a persistent ZK node with the specified boolean value in the given path.
     *
     * @param path path of the ZK node that should be created
     * @param data a boolean value to put in the created ZK node.
     * @throws ZkClientException If can not add ZK node and the error condition is not retry-able or all retries failed.
     */
    public void addPersistentNode(String path, boolean data) throws ZkClientException, InterruptedException {
        addPersistentNode(path, String.valueOf(data));
    }

    /**
     * Sets the specified data in the ZK node with given path.
     *
     * @param path path of the ZK node
     * @param data new data of the ZK node
     * @throws ZkClientException If can not sets data for the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public void setData(String path, byte[] data) throws ZkClientException, InterruptedException {
        Preconditions.checkArgument(data != null);
        try {
            curator.setData().forPath(path, data);
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ZkClientException("Failed to set data for the ZK node in path: " + path, ex);
        }
    }

    /**
     * Sets the specified string in the ZK node with given path.
     *
     * @param path path of the ZK node
     * @param data new data of the ZK node
     * @throws ZkClientException If can not sets data for the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public void setData(String path, String data) throws ZkClientException, InterruptedException {
        setData(path, data.getBytes());
    }

    /**
     * Sets the specified integer value in the ZK node with given path.
     *
     * @param path path of the ZK node
     * @param data an integer value to put in the created ZK node.
     * @throws ZkClientException If can not sets data for the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public void setData(String path, int data) throws ZkClientException, InterruptedException {
        setData(path, String.valueOf(data));
    }

    /**
     * Sets the specified long value in the ZK node with given path.
     *
     * @param path path of the ZK node
     * @param data a long value to put in the created ZK node.
     * @throws ZkClientException If can not sets data for the ZK node and the error condition is not retry-able or all
     *     retries failed.
     */
    public void setData(String path, long data) throws ZkClientException, InterruptedException {
        setData(path, String.valueOf(data));
    }

    /**
     * Registers connection state callback to be called when there is a state change in the connection.
     */
    public void setConnectionStateChangeCallback(Consumer<ConnectionState> callback) {
        connectionStateChangeCallback = callback;
    }

    /**
     * Removes the ZK node with the specified path, if it exists. Otherwise, does nothing.
     *
     * @param path to the ZK node
     * @throws ZkClientException If can not remove ZK node and the error condition is not retry-able or all retries
     *     failed.
     */
    public void remove(String path) throws ZkClientException, InterruptedException {
        remove(path, false);
    }

    /**
     * Removes the ZK node with the specified path, if it exists. Otherwise, does nothing.
     *
     * @param path to the ZK node
     * @param deletingChildrenIfNeeded Whether allowed to remove children of the path or not.
     * @throws ZkClientException If can not remove ZK node and the error condition is not retry-able or all retries
     *     failed.
     */
    public void remove(String path, boolean deletingChildrenIfNeeded) throws ZkClientException, InterruptedException {
        try {
            if (deletingChildrenIfNeeded) {
                curator.delete().deletingChildrenIfNeeded().forPath(path);
            } else {
                curator.delete().forPath(path);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Node {} removed from ZK.", path);
            }
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            // There is a corner case where Curator throws a ConnectionLoss but actually removes
            // the ZK node, so when we retry we get NoNode exception. In here, if we got an
            // exception but the ZK node does not exist anyway, let it go.
            if (!exists(path)) {
                logger.warn("Error removing {} from ZK; but node does not exist anyway", path, ex);
            } else {
                throw new ZkClientException("Failed to remove ZK node in path: " + path, ex);
            }
        }
    }

    /**
     * Removes child nodes of a ZK node with the given path and keeps the parent node.
     *
     * @param path to the parent ZK node
     */
    public void removeChildren(String path) throws ZkClientException, InterruptedException {
        for (String subPath : getChildren(path)) {
            remove(joinPaths(path, subPath), true);
        }
    }

    /**
     * Removes a groups of ZK nodes and adds a new one, a.k.a., replacing in a single transaction.
     *
     * @param oldPaths Paths of the ZK nodes to remove.
     * @param newPath Path of the new ZK node to add.
     * @param data Data for the new ZK node.
     * @param ephemeral Whether the replacing ZK node should be ephemeral.
     * @throws ZkClientException If can not get the ZK node and the error condition is not retry-able or all retries
     *     failed.
     */
    public void replace(String[] oldPaths, String newPath, byte[] data, boolean ephemeral)
            throws ZkClientException, InterruptedException {
        try {
            CuratorTransactionFinal transaction = curator.inTransaction().create()
                    .withMode(ephemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT)
                    .forPath(newPath, data).and();
            for (String path : oldPaths) {
                transaction = transaction.delete().forPath(path).and();
            }
            transaction.commit();
            if (logger.isDebugEnabled()) {
                logger.debug("ZK nodes {} replaced by {}", Arrays.toString(oldPaths), newPath);
            }
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ZkClientException("Failed to replace ZK nodes " + Arrays.toString(oldPaths) + " by " + newPath,
                    ex);
        }
    }

    /**
     * Moves the ZK node with the given path to the ZK node with the same name in the destination path specified. The
     * operation is performed in a single transaction.
     *
     * @param sourcePath source path
     * @param destPath destination path
     * @return the data of the ZK node which is moved
     * @throws ZkClientException If can not get the ZK node and the error condition is not retry-able or all retries
     *     failed.
     */
    public byte[] move(String sourcePath, String destPath) throws ZkClientException, InterruptedException {
        byte[] data;
        try {
            data = curator.getData().forPath(sourcePath);
            curator.inTransaction()
                    .delete().forPath(sourcePath)
                    .and().create().forPath(destPath, data)
                    .and().commit();
            if (logger.isDebugEnabled()) {
                logger.debug("ZK Node {} moved to {}.", sourcePath, destPath);
            }
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ZkClientException("Failed to move a ZK node from path: " + sourcePath
                    + " to path: " + destPath, ex);
        }
        return data;
    }

    /**
     * Makes a deep copy from a ZK node (source path) to a new node (target path). <br/> For example: <br/> after
     * calling cloneNode("/a/b", "/a/t") on the following tree A, it'll be transformed to the tree B.
     * <pre>
     * Tree A:
     * /
     * └── a
     *     └── b
     *         └── c
     *
     * Tree B:
     * /
     * └── a
     *     ├── b
     *     │   └── c
     *     └── t
     *         └── c
     * </pre>
     * Notes:
     * <ul>
     *  <li>This method copies the tree structure with same name and values.</li>
     *  <li>We can not have <i>an atomic snapshot</i> of the subtree ever, so make a traversed source tree using
     *  multiple requests to ZK.</li>
     *  <li>Creation of the traversed tree performed in an atomic operation.</li>
     *  <li>This method doesn't create parent paths of the target path when they are missing.</li>
     * </ul>
     *
     * @param sourcePath the path to be cloned.
     * @param targetPath the target path.
     */
    public void clone(String sourcePath, String targetPath) throws ZkClientException, InterruptedException {
        // Do a BFS traversal on the source-tree and simultaneously build a transaction to be
        // applied by the curator. While traversing the tree, for each newly visited node, add a create operation
        // to the transaction.
        Deque<Pair<String /* src path */, String /* target path */>> queue = new LinkedList<>();
        queue.add(Pair.of(sourcePath, targetPath));
        CuratorTransactionBridge curatorTransactionBridge;
        try {
            curatorTransactionBridge =
                    curator.inTransaction().create().forPath(targetPath, getData(sourcePath));

            Pair<String, String> node;
            while ((node = queue.pollFirst()) != null) {
                List<String> children = getChildren(node.getLeft());
                for (String child : children) {
                    String sourceChild = joinPaths(node.getLeft(), child);
                    String targetChild = joinPaths(node.getRight(), child);
                    curatorTransactionBridge.and().create().forPath(targetChild, getData(sourceChild));
                    queue.add(Pair.of(sourceChild, targetChild));
                }
            }
            curatorTransactionBridge.and().commit();
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception e) {
            throw new ZkClientException("Failed to commit transaction", e);
        }
    }

    /**
     * Returns a new ZK node cache. The cache keeps data of a ZK node locally. You can add listeners by calling
     * cache.getListenable.addListener(...) and then start the cache. Then you will get notified when any change occur
     * on the path. You can also get the current data from cache by calling cache.getCurrentData(...).
     *
     * @param path The path which its direct children should be cached.
     */
    public NodeCache newNodeCache(String path) {
        return new NodeCache(curator, path);
    }

    /**
     * Returns a new path children cache. The cache keeps all data from all children of a ZK path locally. You can add
     * listeners by calling cache.getListenable.addListener(...) and then start the cache. Then you will get notified
     * when any change occur on the path. You can also get the current data from cache by calling
     * cache.getCurrentData(...).
     *
     * @param path The path which its direct children should be cached.
     */
    public PathChildrenCache newPathChildrenCache(String path) {
        return new PathChildrenCache(curator, path, true /*cache data*/);
    }

    /**
     * Returns a new tree cache. The cache keeps all data from all direct and indirect children of a ZK path locally.
     * You can add listeners by calling cache.getListenable.addListener(...) and then start the cache. Then you will get
     * notified when any change occur on the path. You can also get the current data from cache by calling
     * cache.getCurrentData(...).
     *
     * @param path The path which its direct and indirect children should be cached.
     */
    public TreeCache newTreeCache(String path) {
        return new TreeCache(curator, path);
    }

    /**
     * Returns a new {@link InterProcessMutex} object that uses zookeeper to hold the lock, so all processes in all JVMs
     * that use the same lock will achieve an inter-process critical section. for more detail read {@link
     * InterProcessMutex} doc.
     *
     * @param lockPath path of lock in zookeeper
     * @return a new {@link InterProcessMutex} object
     */
    public InterProcessMutex newInterProcessMutex(String lockPath) {
        return new InterProcessMutex(this.curator, lockPath);
    }

    @Override
    public final void stateChanged(CuratorFramework client, ConnectionState newState) {
        logger.info("ZK connection state changed to {}.", newState);
        if (connectionStateChangeCallback != null) {
            connectionStateChangeCallback.accept(newState);
        }
    }

    /**
     * Wraps runtime exceptions from curator operations to the checked exception {@link ZkClientException}. Note that
     * Curator operations throw {@link Exception} which hides the underlying exception types including {@link
     * InterruptedException}. This method separates {@link InterruptedException} from other exceptions. Additionally it
     * wraps other exceptions in {@link ZkClientException} so that they can be dealt with accordingly.
     */
    public static <T> T wrapCuratorException(SupplierWithException<T, Exception> curatorOperations)
            throws ZkClientException, InterruptedException {
        try {
            return curatorOperations.get();
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new ZkClientException(e);
        }
    }

    /**
     * Wraps runtime exceptions from curator operations to the checked exception {@link ZkClientException}.
     *
     * @see #wrapCuratorException(SupplierWithException)
     */
    public static void wrapCuratorException(RunnableWithException<Exception> curatorOperations)
            throws ZkClientException, InterruptedException {
        try {
            curatorOperations.run();
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new ZkClientException(e);
        }
    }


    /**
     * Joins the sub-paths by putting "/" between them if necessary.
     *
     * @param subPaths sub paths
     * @return the joined path
     */
    private static String joinPaths(String... subPaths) {
        Preconditions.checkArgument(subPaths != null && subPaths.length != 0);
        List<String> pathList = new ArrayList<>();
        // We don't want to prepend a '/' to the result on our own, unless if the first sub-path
        // starts with a '/' itself.
        boolean appendSlash = false;
        if (subPaths[0] != null && subPaths[0].startsWith("/")) {
            appendSlash = true;
        }
        for (int i = 0; i < subPaths.length; i++) {
            String sp = subPaths[i];
            if (Strings.isNullOrEmpty(sp)) {
                throw new AssertionError(String.format("Invalid path: %s", Arrays.toString(subPaths)));
            }
            if (sp.startsWith("/")) {
                sp = sp.substring(1);
            }
            if (sp.endsWith("/")) {
                sp = sp.substring(0, sp.length() - 1);
            }
            if (i != 0 || !sp.isEmpty()) {
                pathList.add(sp);
            }
        }
        String finalPath = Joiner.on("/").join(pathList);
        return appendSlash ? "/" + finalPath : finalPath;
    }
}