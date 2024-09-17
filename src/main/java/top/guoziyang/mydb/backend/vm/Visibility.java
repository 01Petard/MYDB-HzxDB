package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

/**
 * VM 实现了调度序列的可串行化
 */
public class Visibility {

    /**
     * 检查版本跳跃。
     * 检查要修改的数据的最新提交版本的创建者对当前事务是否可见
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0) {
            return false;
        } else {
            long xmax = e.getXmax();
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 实现读已提交的隔离等级
     * 判断某个记录对事务 t 是否可见
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        /*
        判断逻辑：
        (XMIN == Ti and                             // 由Ti创建且
            XMAX == NULL                            // 还未被删除
        )
        or                                          // 或
        (XMIN is commited and                       // 由一个已提交的事务创建且
            (XMAX == NULL or                        // 尚未删除或
            (XMAX != Ti and XMAX is not commited)   // 由一个未提交的事务删除
        ))
         */
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        // 如果记录是 t 创建的，且未被删除，则可见
        if (xmin == xid && xmax == 0) return true;

        // 如果记录是 t 创建的，但已被删除
        if (tm.isCommitted(xmin)) {
            // 如果记录未被删除，则可见
            if (xmax == 0) return true;
            // 如果记录被删除，但不是创建者 t 删除的
            if (xmax != xid) {
                // 如果删除者 t 未提交，则可见
                if (!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 实现可重复读的隔离等级
     * 判断某个记录对事务 t 是否可见
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        /*
        判断逻辑：
        (XMIN == Ti and                 // 由Ti创建且
         (XMAX == NULL or               // 尚未被删除
        ))
        or                              // 或
        (XMIN is commited and           // 由一个已提交的事务创建且
         XMIN < XID and                 // 这个事务小于Ti且
         XMIN is not in SP(Ti) and      // 这个事务在Ti开始前提交且
         (XMAX == NULL or               // 尚未被删除或
          (XMAX != Ti and               // 由其他事务删除但是
           (XMAX is not commited or     // 这个事务尚未提交或
        XMAX > Ti or                    // 这个事务在Ti开始之后才开始或
        XMAX is in SP(Ti)               // 这个事务在Ti开始前还未提交
        ))))
         */
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        // 如果记录是 t 创建的，且未被删除，则可见
        if (xmin == xid && xmax == 0) return true;

        // 如果记录已经被 t 提交，…………
        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if (xmax == 0) return true;
            if (xmax != xid) {
                if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
