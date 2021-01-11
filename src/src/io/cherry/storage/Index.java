package io.cherry.storage;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

// 不支持key为0的情况
// 只维护索引，不维护数据，数据区需自行处理(2G空间问题)
public class Index {
	public static final int HASH_UNIT_SIZE = 16; // key + pos + next
	public static final int HASH_VERSION = 0x3301;
	public static final int HEADER_SIZE = 24;

	private ByteBuffer buffer;
	private IntBuffer headerBuffer;
	private ByteBuffer hashBuffer;
	private ByteBuffer conflictBucketBuffer;
	private ByteBuffer conflictBuffer;
	private ByteBuffer dataBucketBuffer;
	private Bucket conflictBucket;
	private Bucket dataBucket;

	private int totalSize;

	// 以下为header部分
	private int hashVersion;
	private int hashNum; // 建议使用接近(dataNum*2)的一个质数
	private int conflictNum; // 建议使用dataNum
	private int dataNum;
	private int dataSize;
	private int useConflictNum; // 使用中的冲突数量

	// 创建前先通过calSize方法预先计算索引文件大小
	public static int calSize(int hashNum, int conflictNum, int dataNum) {
		// size = header size + hash size + conflict bucket size + conflict size
		// + data bucket size
		int size = HEADER_SIZE + hashNum * HASH_UNIT_SIZE
				+ Bucket.calSize(conflictNum) + (conflictNum + 1)
				* HASH_UNIT_SIZE + Bucket.calSize(dataNum);
		return size;
	}

	public Index(ByteBuffer buffer, int bufferSize, int hashNum,
			int conflictNum, int dataNum, int dataSize, boolean isInit)
			throws StorageException {
		if (!(hashNum > 0 && conflictNum > 0 && dataNum > 0 && bufferSize > 0 && dataSize > 12))
			throw new IllegalArgumentException("argument err. hashNum:"
					+ hashNum + ", confilctNum:" + conflictNum + ", dataNum:"
					+ dataNum + ", BufferSize:" + bufferSize);

		this.totalSize = calSize(hashNum, conflictNum, dataNum);
		if (this.totalSize != bufferSize)
			throw new StorageException("bufferSize err. hashNum=" + hashNum
					+ ", conflictNum=" + conflictNum + ", dataNum=" + dataNum
					+ ", bufferSize=" + bufferSize);

		this.buffer = buffer;
		this.hashNum = hashNum;
		this.conflictNum = conflictNum;
		this.dataNum = dataNum;
		this.dataSize = dataSize;
		this.headerBuffer = this.buffer.asIntBuffer();

		ByteBuffer tmpBuffer = this.buffer.duplicate();
		tmpBuffer.position(HEADER_SIZE);
		tmpBuffer.limit(HEADER_SIZE + hashNum * HASH_UNIT_SIZE);
		this.hashBuffer = tmpBuffer.slice();

		tmpBuffer = this.buffer.duplicate();
		tmpBuffer.position(HEADER_SIZE + hashNum * HASH_UNIT_SIZE);
		tmpBuffer.limit(HEADER_SIZE + hashNum * HASH_UNIT_SIZE
				+ Bucket.calSize(conflictNum));
		this.conflictBucketBuffer = tmpBuffer.slice();

		tmpBuffer = this.buffer.duplicate();
		tmpBuffer.position(HEADER_SIZE + hashNum * HASH_UNIT_SIZE
				+ Bucket.calSize(conflictNum));
		tmpBuffer.limit(HEADER_SIZE + hashNum * HASH_UNIT_SIZE
				+ Bucket.calSize(conflictNum) + (conflictNum + 1)
				* HASH_UNIT_SIZE);
		this.conflictBuffer = tmpBuffer.slice();

		tmpBuffer = this.buffer.duplicate();
		tmpBuffer.position(HEADER_SIZE + hashNum * HASH_UNIT_SIZE
				+ Bucket.calSize(conflictNum) + (conflictNum + 1)
				* HASH_UNIT_SIZE);
		tmpBuffer.limit(this.totalSize);
		this.dataBucketBuffer = tmpBuffer.slice();

		if (isInit)
			this.initialize();
		else
			this.check();

	}

	public boolean isEmpty() {
		return this.dataBucket.isEmpty();
	}

	public boolean isFull() {
		return this.dataBucket.isFull();
	}

	public int getUsedNum() {
		return this.dataBucket.getUsedNum();
	}

	public int getIdleNum() {
		return this.dataBucket.getIdleNum();
	}

	public int size() {
		return this.dataBucket.size();
	}

	// 获取key对应的索引位置
	// 不存在返回－1
	public int getIndex(long key) {
		if (key <= 0)
			throw new IllegalArgumentException("key must > 0. key: " + key);

		int idx = Math.abs((int) (key % this.hashNum));
		ByteBuffer tmpBuffer = this.hashBuffer.duplicate();
		tmpBuffer.position(idx * HASH_UNIT_SIZE);
		tmpBuffer.limit(idx * HASH_UNIT_SIZE + HASH_UNIT_SIZE);
		ByteBuffer bb = tmpBuffer.slice();
		long _key = bb.asLongBuffer().get(0);
		int _pos = bb.asIntBuffer().get(2);
		int _next = bb.asIntBuffer().get(3);

		if (_key == 0) {
			assert (_pos == 0 && _next == 0);
			return -1;
		}

		if (key == _key) {
			assert (_pos > 0);
			return _pos;
		}

		if (_next == 0) {
			return -1;
		}

		// 到冲突区找
		for (int i = 0; i < this.conflictNum; i++) {
			assert (_pos > 0);
			assert (this.conflictBucket.hasLink(_next));
			idx = _next;
			tmpBuffer = this.conflictBuffer.duplicate();
			tmpBuffer.position(idx * HASH_UNIT_SIZE);
			tmpBuffer.limit(idx * HASH_UNIT_SIZE + HASH_UNIT_SIZE);
			bb = tmpBuffer.slice();
			_key = bb.asLongBuffer().get(0);
			_pos = bb.asIntBuffer().get(2);
			_next = bb.asIntBuffer().get(3);
			assert (_key > 0 && _pos > 0);

			if (key == _key) {
				assert (_pos > 0);
				return _pos;
			}

			if (_next == 0) {
				return -1;
			}
		}

		return -1;
	}

	// 须先getIndex，当不存在时调用insertData申请一个data空间，然后写数据，然后调用insertIndex写索引
	// 如果写索引失败，需要回收data空间
	public int insertData() throws StorageException {
		return dataBucket.alloc();
	}

	// 先释放索引再释放data
	public boolean freeData(int pos) {
		return dataBucket.free(pos);
	}

	// 请先getIndex，当不存在时调用InsertData申请一个data空间，然后写数据，然后调用InsertIndex写索引
	// 如果写索引失败，需要回收data空间
	public void insertIndex(long key, int pos) throws StorageException {
		if (key <= 0 || pos <= 0) {
			throw new IllegalArgumentException("arguemnt err. key:" + key
					+ ", pos:" + pos);
		}

		int idx = Math.abs((int) (key % this.hashNum));
		ByteBuffer tmpBuffer = this.hashBuffer.duplicate();
		tmpBuffer.position(idx * HASH_UNIT_SIZE);
		tmpBuffer.limit(idx * HASH_UNIT_SIZE + HASH_UNIT_SIZE);
		ByteBuffer bb = tmpBuffer.slice();
		long _key = bb.asLongBuffer().get(0);
		int _pos = bb.asIntBuffer().get(2);
		int _next = bb.asIntBuffer().get(3);
		assert (key != _key);

		int newIdx = 0;

		// 如果有冲突，要在冲突区新建一个索引
		if (_key > 0) {
			assert (_pos > 0);
			newIdx = this.conflictBucket.alloc();
			assert (newIdx > 0);
			ByteBuffer _tmpBuffer = this.conflictBuffer.duplicate();
			_tmpBuffer.position(newIdx * HASH_UNIT_SIZE);
			_tmpBuffer.limit(newIdx * HASH_UNIT_SIZE + HASH_UNIT_SIZE);
			ByteBuffer _bb = _tmpBuffer.slice();
			_bb.asLongBuffer().put(0, _key);
			_bb.asIntBuffer().put(2, _pos);
			_bb.asIntBuffer().put(3, _next);
			this.useConflictNum++;
			headerBuffer.put(5, this.useConflictNum);
		}

		// 写hash区
		bb.asLongBuffer().put(0, key);
		bb.asIntBuffer().put(2, pos);
		bb.asIntBuffer().put(3, newIdx);
	}

	// 先释放索引再释放data
	public boolean freeIndex(long key) {
		if (key <= 0)
			throw new IllegalArgumentException("agument err. key:" + key);

		int idx = Math.abs((int) (key % this.hashNum));
		ByteBuffer tmpBuffer = this.hashBuffer.duplicate();
		tmpBuffer.position(idx * HASH_UNIT_SIZE);
		tmpBuffer.limit(idx * HASH_UNIT_SIZE + HASH_UNIT_SIZE);
		ByteBuffer bb = tmpBuffer.slice();
		long _key = bb.asLongBuffer().get(0);
		int _pos = bb.asIntBuffer().get(2);
		int _next = bb.asIntBuffer().get(3);

		ByteBuffer _bb = bb;
		ByteBuffer preBb = null, nextBb = null;
		long preKey = 0, nextKey = 0;
		int nextIdx = 0;
		int nextPos = 0;
		int nextNext = 0;
		boolean found = false;
		int i = 0;

		for (; i < this.conflictNum; i++) {
			// not found
			if (_key == 0) {
				assert (_pos == 0 && _next == 0);
				return false;
			}

			// found
			if (key == _key) {
				assert (_pos > 0);
				found = true;
				if (_next > 0) {
					assert (this.conflictBucket.hasLink(_next));
					nextIdx = _next;
					tmpBuffer = this.conflictBuffer.duplicate();
					tmpBuffer.position(nextIdx * HASH_UNIT_SIZE);
					tmpBuffer.limit(nextIdx * HASH_UNIT_SIZE + HASH_UNIT_SIZE);
					nextBb = tmpBuffer.slice();
					nextKey = nextBb.asLongBuffer().get(0);
					nextPos = nextBb.asIntBuffer().get(2);
					nextNext = nextBb.asIntBuffer().get(3);
					assert (nextKey > 0 && nextPos > 0);
				}
				break;
			}

			// not found
			if (_next == 0) {
				return false;
			}

			assert (this.conflictBucket.hasLink(_next));
			idx = _next;
			preBb = bb;
			preKey = _key;
			tmpBuffer = this.conflictBuffer.duplicate();
			tmpBuffer.position(idx * HASH_UNIT_SIZE);
			tmpBuffer.limit(idx * HASH_UNIT_SIZE + HASH_UNIT_SIZE);
			bb = tmpBuffer.slice();
			_key = bb.asLongBuffer().get(0);
			_pos = bb.asIntBuffer().get(2);
			_next = bb.asIntBuffer().get(3);
			assert (_key > 0 && _pos > 0);
		}

		assert (i < this.conflictNum && found);
		if (preKey > 0) {
			preBb.asIntBuffer().put(3, _next);
			this.conflictBucket.free(idx);
			this.useConflictNum--;
			if (this.useConflictNum < 0)
				this.useConflictNum = 0;
			this.headerBuffer.put(5, this.useConflictNum);
		} else {
			_bb.asLongBuffer().put(0, nextKey);
			_bb.asIntBuffer().put(2, nextPos);
			_bb.asIntBuffer().put(3, nextNext);
			if (nextKey > 0) {
				this.conflictBucket.free(nextIdx);
				this.useConflictNum--;
				if (this.useConflictNum < 0)
					this.useConflictNum = 0;
				this.headerBuffer.put(5, this.useConflictNum);
			}
		}

		return true;
	}

	@Override
	public String toString() {
		return "Index [version=" + this.hashVersion + " ,datanum="
				+ this.dataNum + " ,datasize=" + this.dataSize + " ,hashNum="
				+ this.hashNum + " ,conflict=" + this.conflictNum + " . "
				+ "used=" + this.getUsedNum() + " ,idle=" + this.getIdleNum()
				+ " ,conflict=" + this.useConflictNum + "]";
	}

	public void setDataLink(int idx, int next) {
		this.dataBucket.setLink(idx, next);
	}

	public void setDataLinkUsed(int idx) {
		this.dataBucket.setLinkUsed(idx);
	}

	public void setDataUsedAndLink(int usedNum, int linkBegin, int linkEnd) {
		this.dataBucket.setUsedAndLink(usedNum, linkBegin, linkEnd);
	}

	private void initialize() throws StorageException {
		if (this.headerBuffer.get(0) != 0)
			throw new StorageException(
					"Index initialize failed: hashVersion must be 0");

		this.hashVersion = HASH_VERSION;
		this.headerBuffer.put(0, this.hashVersion);
		this.headerBuffer.put(1, this.hashNum);
		this.headerBuffer.put(2, this.conflictNum);
		this.headerBuffer.put(3, this.dataNum);
		this.headerBuffer.put(4, this.dataSize);
		this.useConflictNum = 0;
		this.headerBuffer.put(5, this.useConflictNum);

		this.conflictBucket = new Bucket(this.conflictBucketBuffer,
				Bucket.calSize(this.conflictNum), this.conflictNum, true);
		this.dataBucket = new Bucket(this.dataBucketBuffer,
				Bucket.calSize(this.dataNum), this.dataNum, true);
	}

	private void check() throws StorageException {
		this.hashVersion = this.headerBuffer.get(0);
		int hashNum = this.headerBuffer.get(1);
		int conflictNum = this.headerBuffer.get(2);
		int dataNum = this.headerBuffer.get(3);
		int dataSize = this.headerBuffer.get(4);
		this.useConflictNum = this.headerBuffer.get(5);

		if (hashVersion != HASH_VERSION) {
			throw new StorageException("Index check failed: hashVersion="
					+ hashVersion);
		}
		if (this.hashNum != hashNum) {
			throw new StorageException("Index check failed: hashNum=" + hashNum
					+ "!=" + this.hashNum);
		}
		if (this.conflictNum != conflictNum) {
			throw new StorageException("Index check failed: conflictNum="
					+ conflictNum + "!=" + this.conflictNum);
		}
		if (this.dataNum != dataNum) {
			throw new StorageException("Index check failed: dataNum=" + dataNum
					+ "!=" + this.dataNum);
		}
		if (this.dataSize != dataSize) {
			throw new StorageException("Index check failed: dataSize="
					+ dataSize + "!=" + this.dataSize);
		}
		this.conflictBucket = new Bucket(this.conflictBucketBuffer,
				Bucket.calSize(conflictNum), conflictNum, false);
		this.dataBucket = new Bucket(this.dataBucketBuffer,
				Bucket.calSize(dataNum), dataNum, false);
	}

	public static void main(String[] args) {
		int num = 36000000;
		System.out.println("size="
				+ Index.calSize(Utils.getlargerPrime(num * 2), num / 2, num));
	}
}
