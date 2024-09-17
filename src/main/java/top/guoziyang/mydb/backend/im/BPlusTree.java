package top.guoziyang.mydb.backend.im;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.im.Node.InsertAndSplitRes;
import top.guoziyang.mydb.backend.im.Node.LeafSearchRangeRes;
import top.guoziyang.mydb.backend.im.Node.SearchNextRes;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {
    // 数据管理器
    DataManager dm;
    // 根节点的 UID 存储位置的 UID
    long bootUid;

    // 存储根节点 UID 的 DataItem 对象
    DataItem bootDataItem;
    // 用于同步访问根节点 UID 的锁
    Lock bootLock = new ReentrantLock();

    /**
     * 创建一个新的 B+树实例
     * @param dm 数据管理器
     * @return 新树的 UID
     */
    public static long create(DataManager dm) throws Exception {
        // 创建一个空的根节点
        byte[] rawRoot = Node.newNilRootRaw();
        // 插入根节点并获取 UID
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        // 插入 DataItem 来存储根节点的 UID
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    /**
     * 加载一个现有的 B+树实例
     * @param bootUid 根节点 UID 存储位置的 UID
     * @param dm 数据管理器
     * @return BPlusTree 实例
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        // 读取存储根节点 UID 的 DataItem
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree tree = new BPlusTree();
        tree.bootUid = bootUid;
        tree.dm = dm;
        tree.bootDataItem = bootDataItem;
        tree.bootLock = new ReentrantLock();
        return tree;
    }

    /**
     * 获取当前树的根节点 UID
     * @return 根节点 UID
     */
    private long rootUid() {
        bootLock.lock();
        try {
            // 获取存储根节点 UID 的 DataItem 数据
            SubArray sa = bootDataItem.data();
            // 解析 UID
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 更新根节点 UID
     * @param left 左子树的 UID
     * @param right 右子树的 UID
     * @param rightKey 右子树的键值
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            // 创建新的根节点
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            // 插入新的根节点
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            // 更新存储根节点 UID 的 DataItem
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 查找叶子节点
     * @param nodeUid 节点 UID
     * @param key 寻找的键值
     * @return 叶子节点的 UID
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * 查找下一个节点
     * @param nodeUid 节点 UID
     * @param key 寻找的键值
     * @return 下一个节点的 UID
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            SearchNextRes res = node.searchNext(key);
            node.release();
            if (res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    /**
     * 查找键值对应的 UID
     * @param key 寻找的键值
     * @return 包含 UID 的列表
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 查找键值范围内的 UID
     * @param leftKey 左侧键值
     * @param rightKey 右侧键值
     * @return 包含 UID 的列表
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while (true) {
            Node leaf = Node.loadNode(this, leafUid);
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if (res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    /**
     * 插入键值对
     * @param key 键值
     * @param uid UID
     */
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        if (res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    /**
     * 递归插入
     * @param nodeUid 节点 UID
     * @param uid UID
     * @param key 键值
     * @return 插入结果
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if (isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if (ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    /**
     * 插入并可能分割节点
     * @param nodeUid 节点 UID
     * @param uid UID
     * @param key 键值
     * @return 插入结果
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if (iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    /**
     * 关闭 B+树实例
     */
    public void close() {
        bootDataItem.release();
    }

    /**
     * 插入结果类
     */
    static class InsertRes {
        long newNode, newKey;
    }
}