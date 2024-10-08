package top.guoziyang.mydb.backend.tbm;

import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.parser.statement.*;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.backend.vm.VersionManager;
import top.guoziyang.mydb.common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 直接被最外层 Server 调用，直接返回执行的结果
 */
public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    private final Booter booter;
    private final Map<String, Table> tableCache;
    private final Map<Long, List<Table>> xidTableCache;
    private final Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    /**
     * 启动时
     */
    private void loadTables() {
        long uid = firstTableUid();
        while (uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if (t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] showTables(long xid) throws Exception {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            if (tableCache.isEmpty()) {
                return "No tables found.".getBytes(); // 如果没有表，返回提示信息
            }
            for (String tableName : tableCache.keySet()) {
                sb.append(tableName).append("\n"); // 将每个表名追加到字符串
            }
            return sb.toString().getBytes(); // 返回字节数组
        } finally {
            lock.unlock();
        }
    }


    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if (tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if (!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        Table table = getTable(insert.tableName);
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    @Override
    public byte[] read(long xid, Select read) throws Exception {
        Table table = getTable(read.tableName);
        return table.read(xid, read).getBytes();
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        Table table = getTable(update.tableName);
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        Table table = getTable(delete.tableName);
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }

    // 获取表名
    private Table getTable(String tableName) throws Exception {
        lock.lock();
        Table table = tableCache.get(tableName);
        lock.unlock();
        if (table == null) {
            throw Error.TableNotFoundException;
        }
        return table;
    }
}
