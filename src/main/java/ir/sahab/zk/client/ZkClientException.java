package ir.sahab.zk.client;

import org.apache.curator.RetryLoop;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

/**
 * Exception type of ZKClient operations.
 */
public class ZkClientException extends Exception {

    private static final long serialVersionUID = -4600793503080584653L;

    private final Code zkErrorCode;
    private final boolean canResolveByRetry;

    public ZkClientException(String msg, Throwable cause) {
        super(msg, cause);
        if (cause instanceof KeeperException) {
            zkErrorCode = ((KeeperException) cause).code();
            canResolveByRetry = RetryLoop.shouldRetry(zkErrorCode.intValue());
        } else {
            zkErrorCode = null;
            canResolveByRetry = false;
        }
    }

    public ZkClientException(String msg) {
        this(msg, null);
    }

    public ZkClientException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    public Code getZkErrorCode() {
        return zkErrorCode;
    }

    public boolean canResolveByRetry() {
        return canResolveByRetry;
    }
}