package org.suche.json;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public final class AppendableOutputStream extends OutputStream {
	private final Appendable target;
	private final CharsetDecoder decoder;
	private final CharBuffer charBuf;
	private final boolean isWriter;

	public AppendableOutputStream(final Appendable target) {
		this.target = target;
		this.isWriter = target instanceof Writer;
		this.decoder = StandardCharsets.UTF_8.newDecoder()
				.onMalformedInput(CodingErrorAction.REPLACE)
				.onUnmappableCharacter(CodingErrorAction.REPLACE);
		this.charBuf = CharBuffer.allocate(8192);
	}

	@Override public void write(final byte[] b, final int off, final int len) throws IOException {
		final var byteBuf = ByteBuffer.wrap(b, off, len);
		while (byteBuf.hasRemaining()) {
			charBuf.clear();
			decoder.decode(byteBuf, charBuf, false);
			charBuf.flip();
			if (charBuf.hasRemaining()) {
				if (isWriter) {
					((Writer) target).write(charBuf.array(), charBuf.position(), charBuf.remaining());
				} else {
					target.append(charBuf);
				}
			}
		}
	}

	@Override public void write(final int b) throws IOException { write(new byte[] { (byte) b }, 0, 1); }
	@Override public void flush() throws IOException {
		charBuf.clear();
		decoder.decode(ByteBuffer.allocate(0), charBuf, true);
		decoder.flush(charBuf);
		charBuf.flip();
		if (charBuf.hasRemaining()) {
			if (isWriter) ((Writer) target).write(charBuf.array(), charBuf.position(), charBuf.remaining());
			else target.append(charBuf);
		}
		if (target instanceof final java.io.Flushable f) f.flush();
		decoder.reset();
	}

	@Override public void close() throws IOException { flush();
	if (target instanceof final AutoCloseable c) { try { c.close(); } catch (final Exception e) { throw new IOException(e); } }
	}
}