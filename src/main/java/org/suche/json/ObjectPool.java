package org.suche.json;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Elastic Soft-Limit Pool */
final class ObjectPool<T> {
	interface CleanOnRelease { void clean(); }

	private final ConcurrentLinkedDeque<T> pool;
	private final Supplier<T>              factory;
	private final int                      limit;
	private final AtomicInteger            count;

	public ObjectPool(final int pLimit, final Supplier<T> pFactory) {
		this.limit = pLimit;
		this.factory = pFactory;
		this.pool = new ConcurrentLinkedDeque<>();
		this.count = new AtomicInteger(pLimit);
		for (var i = 0; i < pLimit; ++i) {
			this.pool.addFirst(pFactory.get());
		}
	}

	public T acquire() {
		final var t = this.pool.pollFirst();
		if (t != null) {
			this.count.decrementAndGet();
			return t;
		}
		return this.factory.get();
	}

	public void release(final T object) {
		if (object == null) return;
		if (object instanceof final CleanOnRelease e) e.clean();

		if (this.count.get() < this.limit) {
			this.pool.addFirst(object);
			this.count.incrementAndGet();
		}
	}

	public int getCurrentSize() { return this.count.get(); }
}