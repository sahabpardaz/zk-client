package ir.sahab.zk.client;

import org.apache.curator.RetryLoop;
import org.apache.zookeeper.KeeperException;

/**
 * Exception type of ZKClient operations.
 */
public class ZkClientException extends Exception {

    private static final long serialVersionUID = -4600793503080584653L;

    private KeeperException.Code zkErrorCode = null;
    private boolean canResolveByRetry = false;

    public ZkClientException(String msg, Throwable cause) {
        super(msg, cause);
        if (cause instanceof KeeperException) {
            zkErrorCode = ((KeeperException) cause).code();
            canResolveByRetry = RetryLoop.shouldRetry(zkErrorCode.intValue());
        }
    }

    public ZkClientException(String msg) {
        super(msg);
    }

    public ZkClientException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    public KeeperException.Code getZkErrorCode() {
        return zkErrorCode;
    }

    public boolean canResolveByRetry() {
        return canResolveByRetry;
    }
}