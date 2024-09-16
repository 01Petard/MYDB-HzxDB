package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * 事务类，由VM层调用
 * 每个事务对应一个Transaction对象
 */
public class Transaction {
    /**
     * 事务的唯一xid
     */
    public long xid;
    /**
     * 事务的隔离级别，0:READ_COMMITTED，1:REPEATABLE_READ
     */
    public int level;
    /**
     * 快照
     */
    public Map<Long, Boolean> snapshot;
    /**
     * 错误信息
     */
    public Exception err;
    /**
     * 是否自动回滚
     */
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if (level != 0) {
            t.snapshot = new HashMap<>();
            for (Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if (xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
