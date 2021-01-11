package io.cherry.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

// Tool class
// 1. 遍历数据(遍历模式取到的ByteBuffer前8个字节为key)
// 2. 建立索引
// 3. 修改数据块数量或大小
public class StorageKit {
	private static final long MAX_FILE_SIZE = 0x7FFFFFFF; // 单个数据文件最大支持2G

	private String fileName;
	private int dataNum;
	private int dataSize;

	private MmapFile[] dataFiles;
	private ByteBuffer[] dataBuffers;
	private int dataNumOfOneFile;
	private int dataFileNum;

	// fileName不要加后缀
	public StorageKit(String fileName, int dataNum, int dataSize)
			throws StorageException, IOException {
		if (dataNum <= 0 || dataSize <= 0)
			throw new IllegalArgumentException("argument err. dataNum:"
					+ dataNum + ", dataSize:" + dataSize);

		this.fileName = fileName;
		this.dataNum = dataNum;
		this.dataSize = dataSize + 12;
		this.dataNumOfOneFile = (int) (MAX_FILE_SIZE / this.dataSize);
		this.dataNum = 0;

		long totalSize = 1L * (dataNum + 1) * this.dataSize;
		long size = 0;
		long lastFileSize = 0;
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
			this.dataNum++;
		}
		System.out.println("dataFileNum:" + this.dataFileNum
				+ ", dataNumOfOneFile:" + this.dataNumOfOneFile);

		this.dataFiles = new MmapFile[this.dataFileNum];
		this.dataBuffers = new ByteBuffer[this.dataFileNum];
		for (int i = 0; i < this.dataFileNum; i++) {
			File f = new File(fileName + ".dat" + i);
			if (!f.exists())
				throw new StorageException(fileName + ".dat" + i
						+ " not exists.");
			if (i == dataFileNum - 1) {
				this.dataFiles[i] = new MmapFile(f, (int) lastFileSize);
			} else {
				this.dataFiles[i] = new MmapFile(f, this.dataSize
						* this.dataNumOfOneFile);
			}
			this.dataBuffers[i] = this.dataFiles[i].getBuffer();
		}
	}

	/**
	 * 用于按以下遍历模式访问数据区，返回null不代表下一个索引位置也没有数据，可继续往下访问
	 * 
	 * <pre>
	 * for (int i = 0; i &lt; dataNum; i++) {
	 * 	ByteBuffer data = getDataBuffer(i);
	 * 	if (data == null)
	 * 		continue;
	 * 	// do it,可直接修改数据,注意不要修改key部分
	 * }
	 * </pre>
	 * 
	 * @param idx
	 * @return ByteBuffer 前8个字节为key，接着是4个字节的长度，再接着是data内容，整个dataSize的最后4个字节是时间戳
	 */
	public ByteBuffer getDataBuffer(int idx) {
		if (idx > this.dataNum) {
			throw new IllegalArgumentException("idx[" + idx + "] > dataNum["
					+ this.dataNum + "]");
		}
		int pos = idx + 1;
		ByteBuffer tmpBuffer = this.dataBuffers[(pos / this.dataNumOfOneFile)]
				.duplicate();
		tmpBuffer.position((pos % this.dataNumOfOneFile) * this.dataSize);

		tmpBuffer.limit((pos % this.dataNumOfOneFile) * dataSize + 8);
		if (tmpBuffer.slice().asLongBuffer().get(0) == 0)
			return null;

		tmpBuffer = dataBuffers[(pos / this.dataNumOfOneFile)].duplicate();
		tmpBuffer.position((pos % this.dataNumOfOneFile) * this.dataSize);
		tmpBuffer.limit(((pos % this.dataNumOfOneFile) + 1) * this.dataSize);
		return tmpBuffer.slice();
	}

	public byte[] getNotExpiredData(int pos, int expireTime) {
		ByteBuffer tmpBuffer = this.dataBuffers[pos / this.dataNumOfOneFile]
				.duplicate();
		tmpBuffer.position((pos % this.dataNumOfOneFile) * this.dataSize);
		tmpBuffer.limit((pos % this.dataNumOfOneFile) + 12);
		long id = tmpBuffer.slice().asLongBuffer().get(0);
		int len = tmpBuffer.slice().asIntBuffer().get(2);

		if (id == 0)
			return null;

		ByteBuffer _tmpBuffer = this.dataBuffers[pos / this.dataNumOfOneFile]
				.duplicate();
		_tmpBuffer.position((pos % this.dataNumOfOneFile + 1) * this.dataSize
				- 4);
		_tmpBuffer.limit((pos % this.dataNumOfOneFile + 1) * this.dataSize + 12
				+ 4);
		int time = _tmpBuffer.slice().asIntBuffer().get(0);

		if (time < expireTime)
			return null;

		tmpBuffer = dataBuffers[pos / this.dataNumOfOneFile].duplicate();
		tmpBuffer.position((pos % this.dataNumOfOneFile) * dataSize + 12);
		tmpBuffer.limit((pos % this.dataNumOfOneFile) * dataSize + 12 + len);
		ByteBuffer bb = tmpBuffer.slice();

		if (bb == null)
			return null;

		byte[] bytes = new byte[bb.capacity()];
		bb.slice().get(bytes);
		return bytes;
	}

	// 重建索引，文件名不要添加后缀
	public void rebuildIndex() throws StorageException, IOException {
		int hashNum = Utils.getlargerPrime(this.dataNum * 2);
		int conflictNum = Math.abs(this.dataNum / 2);

		int indexSize = Index.calSize(hashNum, conflictNum, this.dataNum);
		File f = new File(this.fileName + ".idx");
		if (f.exists())
			throw new StorageException(
					"StorageKit rebuild index failed: index file[" + fileName
							+ ".idx] exists.");
		MmapFile indexFile = new MmapFile(f, indexSize);
		ByteBuffer buffer = indexFile.getBuffer();
		Index index = new Index(buffer, indexSize, hashNum, conflictNum,
				this.dataNum, this.dataSize, true);

		long startTime = System.currentTimeMillis();
		System.out.println("rebuild index start at: " + startTime);
		int buildNum = 0;
		int nextLink = 0;
		int beginLink = 0, endLink = 0;
		for (int i = this.dataNum; i > 0; i--) {
			ByteBuffer data = this.getDataBuffer(i - 1);
			if (data != null) {
				long key = data.asLongBuffer().get(0);
				if (key > 0) {
					++buildNum;
					index.insertIndex(key, i);
					index.setDataLinkUsed(i);
				} else {
					System.out.println("warning: data invalid at " + i);
					if (nextLink == 0)
						endLink = i;
					index.setDataLink(i, nextLink);
					nextLink = i;
				}
			} else {
				if (nextLink == 0)
					endLink = i;
				index.setDataLink(i, nextLink);
				nextLink = i;
			}
		}

		if (buildNum == 0) {
			beginLink = 0;
			endLink = 0;
		} else {
			beginLink = nextLink;
		}
		index.setDataUsedAndLink(buildNum, beginLink, endLink);
		long endTime = System.currentTimeMillis();
		System.out.println("rebuild index succ. use " + (endTime - startTime)
				+ " ms, buildNum = " + buildNum);
		System.out.println(index.toString());
	}

	public void modifyDataFile(String newFileName, int newDataNum,
			int newDataSize) throws StorageException, IOException {
		if (newDataNum <= 0 && newDataSize <= 0)
			throw new IllegalArgumentException("argument err. newDataNum:"
					+ newDataNum + ", newDataSize:" + newDataSize);

		long startTime = System.currentTimeMillis();
		System.out.println("rebuild index start at: " + startTime);

		newDataSize = newDataSize + 12;
		int newDataNumOfOneFile = (int) (MAX_FILE_SIZE / newDataSize);

		ByteBuffer[] newDataBuffers = createNewFiles(newFileName, newDataNum,
				newDataSize);

		int _num = this.dataNum > newDataNum ? newDataNum : this.dataNum;
		int _size = this.dataSize > newDataSize ? newDataSize : this.dataSize;
		int usedNum = 0;
		int rebuildNum = 0;
		ByteBuffer _buffer = null, buffer_ = null;
		int idx = 1;
		for (int i = 1; i <= this.dataNum; i++) {
			if (idx > _num) {
				if (i < this.dataNum)
					System.out
							.println("newDataNum < dataNum, maybe not deal over.");
				break;
			}
			_buffer = this.dataBuffers[i / this.dataNumOfOneFile].duplicate();
			_buffer.position((i % this.dataNumOfOneFile) * this.dataSize);
			_buffer.limit((i % this.dataNumOfOneFile) * this.dataSize + 8);
			long _key = _buffer.slice().asLongBuffer().get(0);

			if (_key > 0) {
				_buffer = this.dataBuffers[i / this.dataNumOfOneFile]
						.duplicate();
				_buffer.position((i % this.dataNumOfOneFile) * this.dataSize);
				_buffer.limit((i % this.dataNumOfOneFile) * this.dataSize
						+ _size);

				buffer_ = newDataBuffers[idx / newDataNumOfOneFile].duplicate();
				buffer_.position((idx % newDataNumOfOneFile) * newDataSize);
				buffer_.limit((idx % this.dataNumOfOneFile) * newDataSize
						+ _size);
				buffer_.slice().put(_buffer.slice());
				idx++;
				usedNum++;
				rebuildNum++;
			}
		}

		long endTime = System.currentTimeMillis();
		System.out.println("modifyDataFile succ. use " + (endTime - startTime)
				+ " ms");
		System.out.println("oldNum=" + this.dataNum + ", oldSize="
				+ this.dataSize + ", newNum=" + newDataNum + ", newSize="
				+ newDataSize + " ,usedNum=" + usedNum + " ,rebuildNum="
				+ rebuildNum);
	}

	private ByteBuffer[] createNewFiles(String newFileName, int newDataNum,
			int newDataSize) throws StorageException, IOException {
		MmapFile[] newDataFiles;
		ByteBuffer[] newDataBuffers;
		long newTotalSize = 1L * (newDataNum + 1) * newDataSize;
		long newSize = 0;
		long newLastFileSize = 0;
		int newDataNumOfOneFile = (int) (MAX_FILE_SIZE / newDataSize);
		int newDataFileNum = 0;
		for (;;) {
			if (newTotalSize <= newSize) {
				break;
			}
			long left = newTotalSize - newSize;
			if (left > (newDataNumOfOneFile * newDataSize)) {
				left = newDataNumOfOneFile * newDataSize;
			} else {
				newLastFileSize = left;
			}
			newSize += left;
			newDataFileNum++;
		}
		System.out.println("NewFileNum: " + newDataFileNum
				+ ", newDataNumOfOneFile: " + newDataNumOfOneFile);

		newDataFiles = new MmapFile[newDataFileNum];
		newDataBuffers = new ByteBuffer[newDataFileNum];
		for (int i = 0; i < newDataFileNum; i++) {
			File f = new File(newFileName + ".dat" + i);
			if (f.exists())
				throw new StorageException(newFileName + ".dat" + i
						+ " is aready exists.");
			if (i == newDataFileNum - 1) {
				newDataFiles[i] = new MmapFile(f, (int) newLastFileSize);
			} else {
				newDataFiles[i] = new MmapFile(f, newDataNumOfOneFile
						* newDataSize);
			}
			newDataBuffers[i] = newDataFiles[i].getBuffer();
		}

		return newDataBuffers;
	}

}
