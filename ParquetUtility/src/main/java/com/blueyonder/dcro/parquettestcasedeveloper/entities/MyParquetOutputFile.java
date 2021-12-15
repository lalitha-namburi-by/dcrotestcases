package com.blueyonder.dcro.parquettestcasedeveloper.entities;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class MyParquetOutputFile implements OutputFile {

	private OutputStream outputStream;

	public MyParquetOutputFile(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

	@Override
	public PositionOutputStream create(long blockSizeHint) {
		return new BeamOutputStream(outputStream);
	}

	@Override
	public PositionOutputStream createOrOverwrite(long blockSizeHint) {
		return new BeamOutputStream(outputStream);
	}

	@Override
	public boolean supportsBlockSize() {
		return false;
	}

	@Override
	public long defaultBlockSize() {
		return 0;
	}

	private static class BeamOutputStream extends PositionOutputStream {
		private long position = 0;
		private OutputStream outputStream;

		private BeamOutputStream(OutputStream outputStream) {
			this.outputStream = outputStream;
		}

		@Override
		public long getPos() throws IOException {
			return position;
		}

		@Override
		public void write(int b) throws IOException {
			position++;
			outputStream.write(b);
		}

		@Override
		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			outputStream.write(b, off, len);
			position += len;
		}

		@Override
		public void flush() throws IOException {
			outputStream.flush();
		}

		@Override
		public void close() throws IOException {
			outputStream.close();
		}
	}
}



