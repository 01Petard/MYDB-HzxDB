package top.guoziyang.mydb.backend.tbm;

import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.im.BPlusTree;
import top.guoziyang.mydb.backend.parser.statement.*;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.backend.vm.VersionManager;
import top.guoziyang.mydb.common.Error;

import java.util.*;
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
            putTableCache(tb);
        }
    }

    private void putTableCache(Table tb) {
        tableCache.put(tb.name, tb);
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
                throw Error.TableNotFoundException;
            }
            Iterator<String> iterator = tableCache.keySet().iterator();
            while (iterator.hasNext()) {
                String tableName = iterator.next();
                sb.append(tableName);
                if (iterator.hasNext()) {
                    sb.append("\n");
                }
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
            putTableCache(table);
            if (!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 有误！
     * @param xid
     * @param drop
     * @return
     * @throws Exception
     */
    @Override
    public byte[] drop(long xid, Drop drop) throws Exception {
        lock.lock();

        // 检查表是否存在
        Table table = tableCache.get(drop.tableName);
        if (table == null) {
            throw Error.TableNotFoundException;
        }

        try {
            // 从缓存中移除表
            tableCache.remove(table.name);

            // 从事务缓存中移除表
            List<Table> xidTables = xidTableCache.get(xid);
            if (xidTables != null) {
                xidTables.remove(table);
                if (xidTables.isEmpty()) {
                    xidTableCache.remove(xid);
                }
            }

            // 删除表的持久化数据
            vm.delete(xid, table.uid);

            // 删除字段的索引（如果有的话）
            for (Field field : table.fields) {
                if (field.isIndexed()) {
                    BPlusTree bt = field.bt;
                    if (bt != null) {
                        bt.delete(xid);  // 清除索引数据
                    }
                }
            }

            // 删除表的所有数据页
            PageCache pageCache = getTablePageCache(table);
            if (pageCache != null) {
                // 清除所有数据页，将页号回归到 0
                pageCache.truncateByBgno(0);
            }

            // 更新 Booter 文件，移除表的 UID
            long head = updateBooterOnDrop(table.uid);
            updateFirstTableUid(head);


            return ("drop " + table.name).getBytes();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 找到所有属于表的数据页
     * @param table
     * @return
     */
    private PageCache getTablePageCache(Table table) {
        PageCache pageCache = table.getPageCache();
        if (pageCache == null) {
            System.out.println("PageCache is null for table " + table.name);
            return null;
        }

        int maxPageNumber = table.getMaxPageNumber();
        for (int pgno = 1; pgno <= maxPageNumber; pgno++) {
            try {
                Page page = pageCache.getPage(pgno);
                if (page != null) {
                    page.release();  // 释放页面资源
                }
            } catch (Exception e) {
                // 可能有部分页面为空或者无法获取，忽略这些异常
            }
        }
        return pageCache;
    }

    /**
     * 移除表的 UID（未删除，只是移到了尾节点0后面，眼不见心不烦）
     */
    private long updateBooterOnDrop(long uid) {
        long head = firstTableUid();

        // 如果链表为空或只有一个节点，直接返回头节点
        if (head == 0 || head == uid) {
            return head;
        }

        long preUid = head;
        long curUid = Table.loadTable(this, head).nextUid;

        // 找到要删除的节点
        while (curUid != 0 && curUid != uid) {
            preUid = curUid;
            curUid = Table.loadTable(this, curUid).nextUid;
        }

        // 如果找到了要删除的节点
        if (curUid == uid) {
            Table preTable = Table.loadTable(this, preUid);
            Table curTable = Table.loadTable(this, curUid);
            preTable.nextUid = curTable.nextUid; // 跳过当前节点
            curTable.uid = -1; // 将当前节点标记为无效

//            // 从链表中移除节点
//            Table preTable = Table.loadTable(this, preUid);
//            Table curTable = Table.loadTable(this, curUid);
//            preTable.nextUid = curTable.nextUid; // 跳过当前节点
//
//            // 找到链表的尾节点
//            long tailUid = head;
//            while (Table.loadTable(this, tailUid).nextUid != 0) {
//                tailUid = Table.loadTable(this, tailUid).nextUid;
//            }
//
//            // 将要删除的节点放到尾节点后面
//            Table tailTable = Table.loadTable(this, tailUid);
//            tailTable.nextUid = uid;
//
//            // 将被删除的节点放到链表末尾，nextUid设置为0
//            curTable.nextUid = 0;
        }

        // 返回头节点
        return head;
    }

    /**
     * 有误！
     * @param xid
     * @param dropAll
     * @return
     * @throws Exception
     */
    @Override
    public byte[] dropAll(long xid, DropAll dropAll) throws Exception {
        lock.lock();


        try {

            tableCache.forEach((k, v) -> {

                // 从缓存中移除表
                tableCache.remove(k);

                try {
                    // 删除字段的索引（如果有的话）
                    v.fields.forEach(f -> {
                        if (f.isIndexed()) {
                            BPlusTree bt = f.bt;
                            if (bt != null) {
                                try {
                                    bt.delete(xid);  // 清除索引数据
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                // 删除表的所有数据页
                PageCache pageCache = getTablePageCache(v);
                if (pageCache != null) {
                    // 清除所有数据页，将页号回归到 0
                    pageCache.truncateByBgno(0);
                }


                // 删除表的持久化数据
                try {
                    vm.delete(xid, v.uid);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // 从事务缓存中移除表
            xidTableCache.forEach((k, v) -> {
                xidTableCache.remove(k);
            });


            // 更新 Booter 文件，移除表的 UID
            updateFirstTableUid(0L);


            return ("drop all tables").getBytes();
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
