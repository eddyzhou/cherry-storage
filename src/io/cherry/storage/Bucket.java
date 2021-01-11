package io.cherry.storage;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Random;

public class Bucket {
	public static final int BUCKET_LINK_SIZE = 4;
	public static final int BUCKET_VERSION = 0x3201;
	public static final int HEADER_SIZE = 20;

	private static final int INDEX_USEDNUM = 2;
	private static final int INDEX_LINK_BEGIN = 3;
	private static final int INDEX_LINK_END = 4;

	private ByteBuffer buffer;
	private IntBuffer headerBuffer;
	private IntBuffer linkBuffer;
	private int totalSize;

	// 以下为header部分
	private int bucketVersion;
	private int bucketNum;
	private int usedNum;
	private int linkBegin;
	private int linkEnd;

	// 创建前先通过calSize方法预先计算大小,
	// 需要保留一个bucket不用(下标为0的bucket)，每个bucket需要保留4个字节的指针用来标识使用情况
	public static int calSize(int bucketNum) {
		int size = HEADER_SIZE + (bucketNum + 1) * BUCKET_LINK_SIZE;
		return size;
	}

	public Bucket(ByteBuffer buffer, int bufferSize, int bucketNum,
			boolean isInit) throws StorageException {
		if (bucketNum <= 0 || bucketNum >= 0x7FFFFFFF) {
			throw new IllegalArgumentException("bucketNum[" + bucketNum
					+ "] not valid.");
		}

		this.totalSize = calSize(bucketNum);
		if (bufferSize != totalSize)
			throw new StorageException("Bucket size invalid: bucketNum="
					+ bucketNum + ", bufferSize=" + bufferSize);

		this.buffer = buffer;
		this.bucketNum = bucketNum;
		this.headerBuffer = this.buffer.asIntBuffer();
		ByteBuffer tmpBuffer = this.buffer.duplicate();
		tmpBuffer.position(HEADER_SIZE);
		tmpBuffer.limit(HEADER_SIZE + (bucketNum + 1) * BUCKET_LINK_SIZE);
		this.linkBuffer = tmpBuffer.slice().asIntBuffer();

		if (isInit) {
			this.initialize();
		} else {
			this.check();
		}
	}

	public boolean isEmpty() {
		return this.usedNum == 0;
	}

	public boolean isFull() {
		return this.usedNum == bucketNum;
	}

	public int getUsedNum() {
		return this.usedNum;
	}

	public int getIdleNum() {
		return this.bucketNum - this.usedNum;
	}

	public int size() {
		return this.bucketNum;
	}

	// 注意分配的bucket从1开始计算
	public int alloc() throws StorageException {
		if (this.isFull())
			throw new StorageException("Bucket alloc err: Bucket is full.");

		if (this.linkBegin == 0 || this.linkEnd == 0)
			throw new StorageException("Bucket alloc err: linkBegin: "
					+ linkBegin + ", linkEnd: " + linkEnd);

		int pos = this.linkBegin;
		int link = this.linkBuffer.get(pos);
		assert ((link & 0x80000000) == 0);
		int next = (link & 0x7FFFFFFF);
		this.linkBuffer.put(pos, 0x80000000);
		this.linkBegin = next;
		this.usedNum = usedNum + 1;
		this.headerBuffer.put(INDEX_LINK_BEGIN, linkBegin);
		this.headerBuffer.put(INDEX_USEDNUM, usedNum);
		if (next == 0) {
			assert (this.linkEnd == pos);
			this.linkEnd = 0;
			this.headerBuffer.put(INDEX_LINK_END, linkEnd);
			assert (this.isFull());
		}
		assert (pos > 0);
		return pos;
	}

	public boolean hasLink(int idx) {
		if (idx <= 0 || idx > this.bucketNum)
			throw new IllegalArgumentException("idx[" + idx + "] not valid.");

		int link = this.linkBuffer.get(idx);
		if ((link & 0x80000000) == 0)
			return false;

		return true;
	}

	public boolean free(int idx) {
		if (idx <= 0 || idx > this.bucketNum)
			throw new IllegalArgumentException("idx[" + idx + "] not valid.");

		int link = this.linkBuffer.get(idx);
		if ((link & 0x80000000) == 0)
			return false;

		if (this.linkBegin == 0) {
			assert (this.linkEnd == 0);
			assert (this.usedNum == this.bucketNum);
			this.linkBuffer.put(idx, 0);
			this.linkBegin = idx;
			this.linkEnd = idx;
			this.usedNum = this.usedNum - 1;
			this.headerBuffer.put(INDEX_LINK_BEGIN, this.linkBegin);
			this.headerBuffer.put(INDEX_LINK_END, this.linkEnd);
			this.headerBuffer.put(INDEX_USEDNUM, this.usedNum);
		} else {
			this.linkBuffer.put(idx, this.linkBegin);
			this.linkBegin = idx;
			this.usedNum = this.usedNum - 1;
			this.headerBuffer.put(INDEX_LINK_BEGIN, this.linkBegin);
			this.headerBuffer.put(INDEX_USEDNUM, this.usedNum);
		}

		return true;
	}

	// don't use it
	void setLink(int idx, int next) {
		if (next == 0) {
			this.linkEnd = idx;
			this.headerBuffer.put(INDEX_LINK_END, this.linkEnd);
		}
		this.linkBuffer.put(idx, next);
	}

	public void setLinkUsed(int idx) {
		this.linkBuffer.put(idx, 0x80000000);
	}

	public void setUsedAndLink(int usedNum, int linkBegin, int linkEnd) {
		this.linkBegin = linkBegin;
		this.linkEnd = linkEnd;
		this.usedNum = usedNum;

		this.headerBuffer.put(INDEX_LINK_BEGIN, linkBegin);
		this.headerBuffer.put(INDEX_LINK_END, linkEnd);
		this.headerBuffer.put(INDEX_USEDNUM, usedNum);
	}

	@Override
	public String toString() {
		StringBuilder strBu = new StringBuilder();
		strBu.append("Bucket [").append("version=").append(bucketVersion)
				.append(" , bucketNum=").append(bucketNum)
				.append(" , usedNum=").append(usedNum).append(" , linkBegin=")
				.append(linkBegin).append(" , linkEnd=").append(linkEnd);
		return strBu.toString();
	}

	private void initialize() throws StorageException {
		if (this.headerBuffer.get(0) != 0)
			throw new StorageException(
					"Bucket Initialize failed: bucketVersion["
							+ headerBuffer.get(0) + "] is not 0");

		this.bucketVersion = BUCKET_VERSION;
		this.usedNum = 0;
		this.linkBegin = 1; // 0-保留
		this.linkEnd = this.bucketNum;
		this.headerBuffer.put(0, this.bucketVersion);
		this.headerBuffer.put(1, this.bucketNum);
		this.headerBuffer.put(2, this.usedNum);
		this.headerBuffer.put(3, this.linkBegin);
		this.headerBuffer.put(4, this.linkEnd);

		for (int i = 1; i <= this.bucketNum; i++) {
			if (i == this.bucketNum) {
				this.linkBuffer.put(i, 0);
			} else {
				this.linkBuffer.put(i, i + 1);
			}
		}
	}

	private void check() throws StorageException {
		this.bucketVersion = this.headerBuffer.get(0);
		int bucketNum = this.headerBuffer.get(1);
		this.usedNum = this.headerBuffer.get(2);
		this.linkBegin = this.headerBuffer.get(3);
		this.linkEnd = this.headerBuffer.get(4);

		if (this.bucketVersion != BUCKET_VERSION) {
			throw new StorageException("Bucket check failed: bucketVersion="
					+ this.bucketVersion);
		}
		if (this.bucketNum != bucketNum) {
			throw new StorageException("Bucket check failed: bucketNum="
					+ bucketNum + "!=" + this.bucketNum);
		}
		if (this.usedNum > bucketNum) {
			throw new StorageException("Bucket check failed: usedNum="
					+ usedNum + ">" + bucketNum);
		}
		if (this.linkBegin > bucketNum) {
			throw new StorageException("Bucket check failed: linkBegin="
					+ this.linkBegin + ">" + bucketNum);
		}
		if (this.linkEnd > bucketNum) {
			throw new StorageException("Bucket check failed: linkEnd="
					+ this.linkEnd + ">" + bucketNum);
		}

		showCapacity();
	}

	private void showCapacity() {
		int realUsed = 0;
		int idle = 0;

		for (int i = 1; i <= this.bucketNum; i++) {
			int link = this.linkBuffer.get(i);
			if ((link & 0x80000000) == 0) {
				idle++;
			} else {
				realUsed++;
			}
		}

		System.out.println("realUsed: " + realUsed + ", idle: " + idle);
		System.out.println("usedNum=" + this.usedNum + ", bucketNum="
				+ bucketNum + ", linkBegin=" + this.linkBegin + ", linkEnd="
				+ this.linkEnd);
	}

	public static void main(String[] args) throws StorageException {
		int bucketNum = 10000000;
		int bufferSize = Bucket.calSize(bucketNum);
		System.out.println("bucketNum=" + bucketNum + ", bufferSize="
				+ bufferSize);
		ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
		Bucket bucket = new Bucket(buffer, bufferSize, bucketNum, true);
		System.out.println("bucket=" + bucket.toString());
		Random random = new Random();
		for (int i = 0; i < 10000000; i++) {
			int idx = random.nextInt(bucketNum);
			if (idx == 0)
				continue;
			bucket.hasLink(idx);
			if (random.nextInt(10) == 0) {
				bucket.free(idx);
			} else {
				idx = bucket.alloc();
				if (idx == 0) {
					System.out.println("alloc failed.");
				}
			}
		}

		System.out.println("insert over");
		Bucket _bucket = new Bucket(buffer, bufferSize, bucketNum, false);
		System.out.println("_bucket=" + _bucket.toString());
	}
}