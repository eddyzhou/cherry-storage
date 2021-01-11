package io.cherry.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

// 注意:单个文件只支持最大2G
public class MmapFile {
	private final int totalSize;
	private MappedByteBuffer buffer;

	public MmapFile(File mmapfile, int totalSize) throws IOException {
		if (mmapfile == null || totalSize <= 0)
			throw new IllegalArgumentException("parameter err: [mmapfile: "
					+ mmapfile + ", totalSize: " + totalSize + "]");

		this.totalSize = totalSize;

		// mapping file
		RandomAccessFile raf = new RandomAccessFile(mmapfile, "rw");
		FileChannel channel = raf.getChannel();
		this.buffer = mapFile(channel, MapMode.READ_WRITE, 0, totalSize);
		channel.close();
		raf.close();
	}

	private MappedByteBuffer mapFile(FileChannel channel, MapMode mode,
			int position, int size) throws IOException {
		MappedByteBuffer buff = null;
		try {
			buff = channel.map(mode, position, size);
		} catch (IOException e) {
			System.out.println("map file failed, gc and try again.");
			System.gc(); // free file
			// try again
			buff = channel.map(mode, position, size);
		}
		return buff;
	}

	public ByteBuffer getBuffer() {
		return this.buffer.duplicate();
	}

	public int getTotalSize() {
		return this.totalSize;
	}

	// Don't call MappedByteBuffer.force() method to often,this method is meant
	// to force operating system to write content of memory into disk, So if you
	// call force() method each time you write into memory mapped file, you will
	// not see true benefit of using mapped byte buffer, instead it will be
	// similar to disk IO.
	public void forceWrite() {
		if (this.buffer != null)
			this.buffer.force();
	}

	public static void main(String[] args) throws IOException {
		MmapFile mf = new MmapFile(new File(
				"~/dev/cherry-storage/test.m"),
				1024 * 1024 * 10);
		System.out.println("mmap succ");

		IntBuffer ib = mf.getBuffer().asIntBuffer();
		ib.put(2749, 37857);
		System.out.println("buffer:" + ib.get(2749));
	}

}
