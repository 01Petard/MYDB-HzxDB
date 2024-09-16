package top.guoziyang.mydb.backend.server;

import top.guoziyang.mydb.backend.parser.Parser;
import top.guoziyang.mydb.backend.parser.statement.*;
import top.guoziyang.mydb.backend.tbm.BeginRes;
import top.guoziyang.mydb.backend.tbm.TableManager;
import top.guoziyang.mydb.common.Error;

public class Executor {
    TableManager tbm;
    private long xid;
    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    // 退出前，正确地处理完事务
    public void close() {
        if (xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }else {
            System.out.println("Transaction processing..., waiting for commit or abort");
        }
    }

    /**
     * 执行sql，先做事务，再执行语句
     * @param sql
     * @return
     * @throws Exception
     */
    public byte[] execute(byte[] sql) throws Exception {
        Object stat = buildSyntaxTree(sql);
        if (stat instanceof Begin) {
            if (xid != 0) {
                throw Error.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin) stat);
            xid = r.xid;
            return r.result;
        } else if (stat instanceof Commit) {
            if (xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if (stat instanceof Abort) {
            if (xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            return execute2(stat);
        }
    }

    /**
     * 进行词法分析、语法分析，构建语法树
     * @param sql
     * @return
     * @throws Exception
     */
    private Object buildSyntaxTree(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        return Parser.Parse(sql);
    }

    /**
     * 执行sql语句
     * @param stat
     * @return
     * @throws Exception
     */
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if (xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            // 根据stat的类型，执行响应的操作
            byte[] res = null;
            if (stat instanceof Show) {
                res = tbm.show(xid);
            } else if (stat instanceof Create) {
                res = tbm.create(xid, (Create) stat);
            } else if (stat instanceof Select) {
                res = tbm.read(xid, (Select) stat);
            } else if (stat instanceof Insert) {
                res = tbm.insert(xid, (Insert) stat);
            } else if (stat instanceof Delete) {
                res = tbm.delete(xid, (Delete) stat);
            } else if (stat instanceof Update) {
                res = tbm.update(xid, (Update) stat);
            }
            return res;
        } catch (Exception e1) {
            e = e1;
            throw e;
        } finally {
            if (tmpTransaction) {
                if (e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
