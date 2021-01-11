package io.cherry.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Storage {
	private static final long MAX_FILE_SIZE = 0x7FFFFFFF; // 单个数据文件最大支持2G

	private MmapFile indexFile;
	private Index index;
	private MmapFile[] dataFiles;
	private ByteBuffer[] dataBuffers;
	private int dataNumOfOneFile;
	private int dataFileNum;
	private int dataSize;

	private String statFile;
	private long statLastRecordTime;
	private long statGetCount;
	private long statPutCount;
	private long statUseMsec;
	private int statMaxDatasize;

	// fileName不要加后缀，会自动增加idx和dat后缀
	public Storage(String fileName, int dataNum, int dataSize)
			throws StorageException, IOException {
		this.statFile = fileName;
		int hashNum = Utils.getlargerPrime(dataNum * 2);
		int conflictNum = Math.abs(dataNum / 2);
		this.dataSize = dataSize + 12;

		// indexFile
		int indexSize = Index.calSize(hashNum, conflictNum, dataNum);
		File indexFile = new File(fileName + ".idx");
		boolean needInit = !indexFile.exists();
		this.indexFile = new MmapFile(indexFile, indexSize);
		ByteBuffer bb = this.indexFile.getBuffer();
		this.index = new Index(bb, indexSize, hashNum, conflictNum, dataNum,
				this.dataSize, needInit);

		// dataFile
		long totalSize = 1L * (dataNum + 1) * this.dataSize;
		long size = 0;
		long lastFileSize = 0;
		this.dataNumOfOneFile = (int) (MAX_FILE_SIZE / this.dataSize);
		this.dataFileNum = 0;
		for (;;) {
			if (totalSize <= size) {
				break;
			}
			long left = totalSize - size;
			if (left > (this.dataNumOfOneFile * this.dataSize)) {
				left = this.dataNumOfOneFile * this.dataSize;
			} else {
				lastFileSize = left;
			}

			size += left;
			this.dataFileNum++;
		}

		this.dataFiles = new MmapFile[this.dataFileNum];
		this.dataBuffers = new ByteBuffer[this.dataFileNum];
		for (int i = 0; i < this.dataFileNum; i++) {
			if (i == this.dataFileNum - 1) {
				this.dataFiles[i] = new MmapFile(
						new File(fileName + ".dat" + i), (int) lastFileSize);
			} else {
				this.dataFiles[i] = new MmapFile(
						new File(fileName + ".dat" + i), this.dataNumOfOneFile
								* this.dataSize);
			}
			this.dataBuffers[i] = this.dataFiles[i].getBuffer();
		}
	}

	public boolean isEmpty() {
		return this.index.isEmpty();
	}

	public boolean isFull() {
		return this.index.isFull();
	}

	public int getUsedNum() {
		return this.index.getUsedNum();
	}

	public int getIdleNum() {
		return this.index.getIdleNum();
	}

	public int size() {
		return this.index.size();
	}

	public boolean contains(long key) {
		return this.index.getIndex(key) > 0;
	}

	public byte[] get(long key) {
		ByteBuffer bb = getByteBuffer(key);
		if (bb == null)
			return null;

		byte[] bytes = new byte[bb.capacity()];
		bb.slice().get(bytes);
		return bytes;
	}

	public ByteBuffer getByteBuffer(long key) {
		++statGetCount;
		long startTime = System.currentTimeMillis();
		doStat(startTime);
		int pos = index.getIndex(key);
		if (pos <= 0) {
			long endTime = System.currentTimeMillis();
			statUseMsec += (endTime - startTime);
			return null;
		}

		ByteBuffer tmpBuffer = dataBuffers[pos / this.dataNumOfOneFile]
				.duplicate();
		tmpBuffer.position((pos % this.dataNumOfOneFile) * this.dataSize);
		tmpBuffer.limit((pos % this.dataNumOfOneFile) * this.dataSize + 12);
		long _key = tmpBuffer.slice().asLongBuffer().get(0);
		assert (_key == key);
		int len = tmpBuffer.slice().asIntBuffer().get(2);
		assert (len + 12 <= this.dataSize);

		tmpBuffer = this.dataBuffers[pos / this.dataNumOfOneFile].duplicate();
		tmpBuffer.position((pos % this.dataNumOfOneFile) * this.dataSize + 12);
		tmpBuffer.limit((pos % this.dataNumOfOneFile) * this.dataSize + 12
				+ len);

		long endTime = System.currentTimeMillis();
		statUseMsec += (endTime - startTime);
		return tmpBuffer.slice();
	}

	// 存在则覆盖；不存在则新增
	public void put(long key, byte[] bytes) throws StorageException {
		++statPutCount;
		if (bytes.length > this.statMaxDatasize)
			this.statMaxDatasize = bytes.length;
		long startTime = System.currentTimeMillis();
		doStat(startTime);

		if (bytes.length + 16 > this.dataSize) {
			throw new StorageException("Storage put failed: data too big");
		}

		int pos = index.getIndex(key);
		if (pos > 0) {
			ByteBuffer tmpBuffer = this.dataBuffers[(pos / this.dataNumOfOneFile)]
					.duplicate();
			tmpBuffer.position((pos % this.dataNumOfOneFile) * dataSize);
			tmpBuffer.limit((pos % this.dataNumOfOneFile) * dataSize + 12);
			long _key = tmpBuffer.slice().asLongBuffer().get(0);
			assert (_key == key);

			// write len
			// key-长度-data-时间戳，(key-长度-时间戳)部分共占16个字节
			int writeSize = ((this.dataSize - 16) > bytes.length ? bytes.length
					: (this.dataSize - 16));
			tmpBuffer.asIntBuffer().put(2, writeSize);

			// write data
			tmpBuffer = dataBuffers[(pos / this.dataNumOfOneFile)].duplicate();
			tmpBuffer.position((pos % this.dataNumOfOneFile) * this.dataSize
					+ 12);
			tmpBuffer
					.limit(((pos % this.dataNumOfOneFile) + 1) * this.dataSize);
			tmpBuffer.slice().put(bytes, 0, writeSize);

			// 时间戳
			tmpBuffer.position(((pos % this.dataNumOfOneFile) + 1) * dataSize
					- 4);
			tmpBuffer.slice().asIntBuffer().put(0, (int) (startTime / 1000));
		} else {
			int _pos = this.index.insertData();
			ByteBuffer tmpBuffer = this.dataBuffers[_pos
					/ this.dataNumOfOneFile].duplicate();
			tmpBuffer.position((_pos % this.dataNumOfOneFile) * dataSize);
			tmpBuffer.limit((_pos % this.dataNumOfOneFile) * dataSize + 12);
			tmpBuffer.slice().asLongBuffer().put(0, key);

			int writeSize = ((this.dataSize - 16) > bytes.length ? bytes.length
					: (this.dataSize - 16));

			// write len
			tmpBuffer.slice().asIntBuffer().put(2, writeSize);

			// write data
			tmpBuffer = this.dataBuffers[_pos / this.dataNumOfOneFile]
					.duplicate();
			tmpBuffer.position((_pos % this.dataNumOfOneFile) * dataSize + 12);
			tmpBuffer.limit(((_pos % this.dataNumOfOneFile) + 1) * dataSize);
			tmpBuffer.slice().put(bytes, 0, writeSize);

			tmpBuffer.limit(((_pos % this.dataNumOfOneFile) + 1)
					* this.dataSize - 4);
			tmpBuffer.slice().asIntBuffer().put(0, (int) startTime / 1000);

			try {
				this.index.insertIndex(key, _pos);
			} catch (StorageException e) {
				// for reuse
				this.index.freeData(_pos);
				tmpBuffer = this.dataBuffers[_pos / this.dataNumOfOneFile]
						.duplicate();
				tmpBuffer.position((_pos % this.dataNumOfOneFile)
						* this.dataSize);
				tmpBuffer.limit((_pos % this.dataNumOfOneFile) * this.dataSize
						+ 12);
				tmpBuffer.slice().asLongBuffer().put(0, 0); // just for resume
				tmpBuffer.slice().asIntBuffer().put(2, 0); // just for resume
			}
		}

		long endTime = System.currentTimeMillis();
		statUseMsec += (endTime - startTime);
	}

	public void free(long key) throws StorageException {
		++statPutCount;
		long startTime = System.currentTimeMillis();
		doStat(startTime);

		int pos = this.index.getIndex(key);
		if (pos > 0) {
			ByteBuffer tmpBuffer = this.dataBuffers[pos / this.dataNumOfOneFile]
					.duplicate();
			tmpBuffer.position((pos % this.dataNumOfOneFile) * dataSize);
			tmpBuffer.limit((pos % this.dataNumOfOneFile) * dataSize + 12);
			tmpBuffer.slice().asLongBuffer().put(0, 0); // just for resume
			tmpBuffer.slice().asIntBuffer().put(2, 0); // just for resume
			this.index.freeIndex(key);
			this.index.freeData(pos);
		}

		long endTime = System.currentTimeMillis();
		statUseMsec += (endTime - startTime);
	}

	@Override
	public String toString() {
		return index.toString();
	}

	// TODO: System.out.println change to logger
	private void doStat(long time) {
		if (time - statLastRecordTime > 1000 * 60 * 60) {
			statLastRecordTime = time;
			long avg = statUseMsec == 0 ? 0
					: ((statGetCount + statPutCount) * 1000 / statUseMsec);
			long useNumRate = (getUsedNum() * 100) / size();
			long dataSizeRate = (statMaxDatasize * 100) / dataSize;
			System.out.println("[" + statFile + "] stat:" + index.toString()
					+ " , [getCount=" + statGetCount + ", putCount="
					+ statPutCount + ", usedMsec=" + statUseMsec + ", avg="
					+ avg + ", maxDatasize=" + statMaxDatasize
					+ ", useNumRate=" + useNumRate + ", dataSizeRate="
					+ dataSizeRate + "]");

			if (useNumRate > 90) {
				System.out.println("[" + statFile + "] warning: useNumRate="
						+ useNumRate);
			}
			if (dataSizeRate > 90) {
				System.out.println("[" + statFile + "] warning: dataSizeRate="
						+ dataSizeRate);
			}
		}
	}

}
