# Zookeeper Client
[![Tests](https://github.com/sahabpardaz/zk-client/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/sahabpardaz/zk-client/actions/workflows/maven.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_zk-client&metric=coverage)](https://sonarcloud.io/dashboard?id=sahabpardaz_zk-client)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=sahabpardaz_zk-client&metric=alert_status)](https://sonarcloud.io/dashboard?id=sahabpardaz_zk-client)
[![JitPack](https://jitpack.io/v/sahabpardaz/zk-client.svg)](https://jitpack.io/#sahabpardaz/zk-client)

This is a client library for [Apache ZooKeeper](https://zookeeper.apache.org/). In fact, it is a thin wrapper around
 [Apache Curator](https://curator.apache.org/). Compared with Curator, we will see the following differences.

## Checked Exceptions

It prefers checked exceptions over runtime ones to avoid exceptions being missed or unhandled.
So we see `InterruptedException` and `ZkClientException` in signature of the operation calls.
`ZkClientException` is a checked exception that is a wrapper around the 
[KeeperException](https://zookeeper.apache.org/doc/r3.3.3/api/org/apache/zookeeper/KeeperException.html).
It provides two useful methods:

- `getZkErrorCode()`: returns the standard ZK error code extracted from the underlying `KeeperException`.
- `canResolveByRetry()`: Some network related exceptions can be fixed by retry, but error codes like 
`NONODE` or `NODEEXISTS` should be handled differently. This method helps to separate these two categories from each other.

## Concise Operation Calls

The operations are written in a more concise manner. For example the equivalent of curator call

```
curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);
```

is:

```
zkClient.addEphemeralNode(path, data);
```

However, it is possible to go back to the curator style, by getting the underlying curator object from `ZkClient` instance:

```
CuratorFramework curator = zkClient.getUnderlyingCurator()
```

And this way you have access to the full feature list of curator.
