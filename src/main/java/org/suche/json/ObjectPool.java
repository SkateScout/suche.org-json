package org.suche.json;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Elastic Soft-Limit Pool */
final class ObjectPool<T> {
	interface CleanOnRelease { void clean(); }
	private final ConcurrentLinkedQueue<T>   pool   ;
	private final Supplier<T>   factory;
	private final int           limit  ;
	private final AtomicInteger count  ;

	public ObjectPool(final int limit, final Supplier<T> factory) {
		this.limit = limit;
		this.factory = factory;
		this.pool = new ConcurrentLinkedQueue<>();
		this.count = new AtomicInteger(limit);
		for (var i = 0; i < limit; ++i) {
			this.pool.add(factory.get());
		}
	}

	public T acquire() {
		final var t = this.pool.poll();
		if (t != null) {
			this.count.decrementAndGet();
			return t;
		}
		return this.factory.get();
	}

	public void release(final T object) {
		if(object == null) return;
		if(object instanceof final CleanOnRelease e) e.clean();
		// Allow short time more objects, will later with acquire removed.
		if (this.count.get() < this.limit) {
			if(!this.pool.offer(object)) throw new IllegalStateException("Should never happend since unbound ConcurrentLinkedQueue");
			this.count.incrementAndGet();
		}
	}

	public int getCurrentSize() { return this.count.get(); }
}