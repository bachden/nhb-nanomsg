package nhb.nanomsg;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public abstract class NNSender extends NNStartable {

	private NNSocket socket;

	protected abstract NNSocket createSocket();

	private final AtomicLong totalSentBytes = new AtomicLong(0);
	private final AtomicLong totalSentMsg = new AtomicLong(0);

	public final long getTotalSentMsg() {
		return totalSentMsg.get();
	}

	public final long getTotalSentBytes() {
		return this.totalSentBytes.get();
	}

	@Override
	protected final void onStart() {
		this.totalSentBytes.set(0);
		this.totalSentMsg.set(0);

		this.socket = this.createSocket();
		this.onStartSuccess();
	}

	protected void onStartSuccess() {

	}

	@Override
	protected final void onStop() {
		this.socket.close();
		this.socket = null;
	}

	protected void send(ByteBuffer buffer) {
		if (this.isRunning()) {
			final int length = buffer.remaining();

			this.socket.send(buffer);

			this.totalSentBytes.addAndGet(length);
			this.totalSentMsg.incrementAndGet();
		} else {
			throw new IllegalStateException("Not started");
		}
	}
}
