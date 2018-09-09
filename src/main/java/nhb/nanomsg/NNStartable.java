package nhb.nanomsg;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public abstract class NNStartable {

	private final AtomicBoolean started = new AtomicBoolean(false);
	private volatile boolean running = false;

	public final boolean isRunning() {
		while ((this.started.get() && !this.running) || (!this.started.get() && this.running)) {
			LockSupport.parkNanos(10);
		}
		return this.running;
	}

	public final void start() {
		if (!this.isRunning() && started.compareAndSet(false, true)) {
			try {
				this.onStart();
				this.running = true;
			} catch (Exception e) {
				this.started.set(false);
				throw e;
			}
		}
	}

	protected abstract void onStart();

	public final void stop() {
		if (this.isRunning() && started.compareAndSet(true, false)) {
			try {
				this.onStop();
				this.running = false;
			} catch (Exception e) {
				this.started.set(true);
				throw e;
			}
		}
	}

	protected abstract void onStop();
}
