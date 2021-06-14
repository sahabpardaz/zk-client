package ir.sahab.zk.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import ir.sahab.zookeeperrule.ZooKeeperRule;
import java.util.Arrays;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class ZkClientTest {

    private static final String ROOT_PATH = "/base-node";

    private static ZkClient zkClient;

    @ClassRule
    public static ZooKeeperRule zkServer = new ZooKeeperRule();

    @BeforeClass
    public static void beforeAll() throws InterruptedException {
        zkClient = new ZkClient();
        zkClient.start(zkServer.getAddress());
    }

    @After
    public void tearDown() throws Exception {
        if (zkClient.exists(ROOT_PATH)) {
            zkClient.remove(ROOT_PATH, true);
        }
    }

    @AfterClass
    public static void afterAll() {
        zkClient.close();
    }

    @Test
    public void testUsingUnderlyingCurator() throws Exception {
        CuratorFramework curator = zkClient.getUnderlyingCurator();
        curator.create().creatingParentsIfNeeded().forPath(ROOT_PATH + "/test-curator", ("data").getBytes());
        assertEquals("data", new String(curator.getData().forPath(ROOT_PATH + "/test-curator")));
    }

    @Test
    public void testNodeExistence() throws ZkClientException, InterruptedException {
        String path = ROOT_PATH + "/node";
        assertFalse(zkClient.exists(path));
        zkClient.addPersistentNode(path, "data", true);
        assertTrue(zkClient.exists(path));
    }

    @Test
    public void testAddPersistentNode() throws ZkClientException, InterruptedException {
        String path = ROOT_PATH + "/node";
        zkClient.addPersistentNode(path, "data", true);
        assertTrue(zkClient.exists(path));
        assertEquals("data", zkClient.getDataAsString(path));
    }

    @Test
    public void testGetData() throws ZkClientException, InterruptedException {
        String path = ROOT_PATH + "/node";
        zkClient.addPersistentNode(path + "/1", "data".getBytes(), true);
        assertEquals("data", zkClient.getDataAsString(path + "/1"));
        zkClient.addPersistentNode(path + "/2", 110);
        assertEquals(110, zkClient.getDataAsInteger(path + "/2"));
        zkClient.addPersistentNode(path + "/3", 111L);
        assertEquals(111L, zkClient.getDataAsLong(path + "/3"));
        zkClient.addPersistentNode(path + "/4", true);
        assertTrue(zkClient.getDataAsBoolean(path + "/4"));
    }

    @Test
    public void testSetData() throws ZkClientException, InterruptedException {
        String path = ROOT_PATH + "/node";
        zkClient.addPersistentNode(path, "data", true);
        zkClient.setData(path, "updatedData");
        assertEquals("updatedData", zkClient.getDataAsString(path));
        zkClient.setData(path, 110);
        assertEquals(110, zkClient.getDataAsInteger(path));
        zkClient.setData(path, 111L);
        assertEquals(111L, zkClient.getDataAsLong(path));
        zkClient.setData(path, "true");
        assertTrue(zkClient.getDataAsBoolean(path));

    }

    @Test
    public void testAddEphemeralNode() throws ZkClientException, InterruptedException {
        String path = ROOT_PATH + "/node";
        zkClient.addPersistentNode(path, "data", true);
        zkClient.addEphemeralNode(path + "/e-node", "data".getBytes());
        assertTrue(zkClient.exists(path + "/e-node"));
    }

    @Test
    public void testChildrenInfo() throws ZkClientException, InterruptedException {
        // Check children of a node
        String path = ROOT_PATH + "/node";
        zkClient.addPersistentNode(path + "/1", "data".getBytes(), true);
        zkClient.addEphemeralNode(path + "/1/e-node", "data".getBytes());
        zkClient.addPersistentNode(path + "/2", "data".getBytes());
        zkClient.addPersistentNode(path + "/3", "data".getBytes());
        zkClient.addPersistentNode(path + "/4", "data".getBytes());
        List<String> children = zkClient.getChildren(path);
        assertEquals(4, children.size());
        assertEquals(Arrays.asList("1", "2", "3", "4"), children);
        assertEquals(4, zkClient.getNumChildren(path));

        // Check node stat
        Stat stat = zkClient.getStat(path);
        assertNotNull(stat);
        assertEquals(4, stat.getNumChildren());

        // Check sub-paths of a node
        List<String> subPaths = zkClient.getSubPaths(path);
        assertEquals(6, subPaths.size());
        List<String> expectedNodes = Arrays
                .asList(path, path + "/1", path + "/1/e-node", path + "/2", path + "/3", path + "/4");
        assertTrue(subPaths.containsAll(expectedNodes) && expectedNodes.containsAll(subPaths));
    }

    @Test
    public void testReplaceNodes() throws ZkClientException, InterruptedException {
        String path = ROOT_PATH + "/node";
        zkClient.addPersistentNode(path + "/1", "data", true);
        zkClient.replace(new String[] {path + "/1"}, path + "/new-node1", "new-data".getBytes(), false);
        assertFalse(zkClient.exists(path + "/1"));
        assertTrue(zkClient.exists(path + "/new-node1"));
        assertEquals("new-data", zkClient.getDataAsString(path + "/new-node1"));
    }

    @Test
    public void testMoveNode() throws ZkClientException, InterruptedException {
        String path = ROOT_PATH + "/node";
        zkClient.addPersistentNode(path + "/1", "true", true);
        zkClient.move(path + "/1", path + "/new-node1");
        assertFalse(zkClient.exists(path + "/1"));
        assertTrue(zkClient.exists(path + "/new-node1"));
        assertTrue(zkClient.getDataAsBoolean(path + "/new-node1"));
    }

    @Test
    public void testRemoveNodes() throws ZkClientException, InterruptedException {
        String path = ROOT_PATH + "/node";
        zkClient.addPersistentNode(path + "/1", "data", true);
        zkClient.addPersistentNode(path + "/2", "data", true);
        zkClient.addPersistentNode(path + "/3", "data", true);

        // Check removing a node
        zkClient.remove(path + "/1");
        assertFalse(zkClient.exists(path + "/1"));

        // Check removing all children of a parent node
        zkClient.removeChildren(path);
        assertEquals(0, zkClient.getNumChildren(path));

        // Check removing a parent node with its children
        zkClient.addPersistentNode(path + "/1", "data");
        zkClient.addPersistentNode(path + "/2", "data");
        zkClient.remove(path, true);
        assertFalse(zkClient.exists(path));
        assertFalse(zkClient.exists(path + "/1"));
        assertFalse(zkClient.exists(path + "/2"));
    }

    @Test
    public void testDeepCloneNodes() throws ZkClientException, InterruptedException {
        String path = ROOT_PATH + "/node";
        zkClient.addPersistentNode(path, "data", true);
        zkClient.addPersistentNode(path + "/1", "data", true);
        zkClient.addPersistentNode(path + "/2", "data");
        zkClient.addPersistentNode(path + "/2/3", "data");

        String clonePath = ROOT_PATH + "/new-node";
        zkClient.clone(path, clonePath);
        zkClient.exists(clonePath);
        List<String> subPaths = zkClient.getSubPaths(clonePath);
        assertEquals(4, subPaths.size());
        List<String> expectedNodes = Arrays.asList(clonePath, clonePath + "/1", clonePath + "/2", clonePath + "/2/3");
        assertTrue(subPaths.containsAll(expectedNodes) && expectedNodes.containsAll(subPaths));
    }

    @Test(expected = ZkClientException.class)
    public void testAddExistingNode() throws ZkClientException, InterruptedException {
        zkClient.addPersistentNode(ROOT_PATH + "/node-1", "data", true);
        assertTrue(zkClient.exists(ROOT_PATH + "/node-1"));
        zkClient.addPersistentNode(ROOT_PATH + "/node-1", "data", true);
    }

    @Test(expected = ZkClientException.class)
    public void testRemoveNodeWithEphemeralChild() throws ZkClientException, InterruptedException {
        zkClient.addPersistentNode(ROOT_PATH + "/node-with-ephemeral-child", "data", true);
        zkClient.addEphemeralNode(ROOT_PATH + "/node-with-ephemeral-child" + "/1", "data".getBytes());
        zkClient.remove(ROOT_PATH + "/node-with-ephemeral-child");
    }

    @Test(expected = ZkClientException.class)
    public void testRemoveNodeWithPersistentChild() throws ZkClientException, InterruptedException {
        zkClient.addPersistentNode(ROOT_PATH + "/persistent-child", "data", true);
        zkClient.remove(ROOT_PATH);
    }

    @Test(expected = ZkClientException.class)
    public void testGetMissingNode() throws ZkClientException, InterruptedException {
        zkClient.getData(ROOT_PATH + "/missing-node");
    }

    @Test(expected = ZkClientException.class)
    public void testSetMissingNode() throws ZkClientException, InterruptedException {
        zkClient.setData(ROOT_PATH + "/missing-node", "data");
    }

    @Test(expected = ZkClientException.class)
    public void testMoveMissingNode() throws ZkClientException, InterruptedException {
        zkClient.move(ROOT_PATH + "/missing-node", ROOT_PATH + "/new-node");
    }

    @Test(expected = ZkClientException.class)
    public void testMoveToExistingNode() throws ZkClientException, InterruptedException {
        zkClient.addPersistentNode(ROOT_PATH + "/node-1", "data", true);
        zkClient.addPersistentNode(ROOT_PATH + "/node-2");
        zkClient.move(ROOT_PATH + "/node-2", ROOT_PATH + "/node-1");
    }

    @Test(expected = ZkClientException.class)
    public void testReplaceMissingNode() throws ZkClientException, InterruptedException {
        zkClient.replace(new String[] {ROOT_PATH + "/missing-node"}, ROOT_PATH + "/new-node", "data".getBytes(), false);
    }

    @Test(expected = ZkClientException.class)
    public void testCloneFromMissingPath() throws Exception {
        zkClient.clone("/z", "/Z");
    }

    @Test(expected = ZkClientException.class)
    public void testCloneToMissingPath() throws Exception {
        zkClient.addPersistentNode(ROOT_PATH + "/a", "data", true);
        zkClient.clone(ROOT_PATH + "/a", "/X/Y/Z");
    }

    @Test(expected = ZkClientException.class)
    public void testCloneOnSamePath() throws Exception {
        zkClient.addPersistentNode(ROOT_PATH + "/a", "data", true);
        zkClient.clone(ROOT_PATH + "/a", ROOT_PATH + "/a");
    }

    @Test(expected = ZkClientException.class)
    public void testCloneToExistingNode() throws Exception {
        zkClient.addPersistentNode(ROOT_PATH + "/a", "data", true);
        zkClient.addPersistentNode(ROOT_PATH + "/b", "data", true);
        zkClient.clone(ROOT_PATH + "/a", ROOT_PATH + "/b");
    }
}