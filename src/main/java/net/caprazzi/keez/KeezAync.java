package net.caprazzi.keez;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import net.caprazzi.keez.Keez.Db;
import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.GetRevisions;
import net.caprazzi.keez.Keez.List;
import net.caprazzi.keez.Keez.Put;

/**
 * Async wrapper for a Keez instance.
 * Incoming requests are queued and executed one at a time in a single thread.
 * This allows to use a non-thread safe Db instance
 */
public class KeezAync implements Db {

	private BlockingQueue<Request<?>> messages = new LinkedBlockingQueue<Request<?>>();
	
	public KeezAync(final Db db) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.execute(new Runnable() {
			public void run() {
				while(true) {
					try {
						Request<?> request = messages.take();
						request.dispatch(db);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			}
		});
	}
	
	/**
	 * Note how each method invokes callback.error in case of interrupedException.
	 * This respects the Db contract, but the application is likely to go down anyway.
	 */
	
	@Override
	public void put(String key, int rev, byte[] body, Put callback) {
		try {
			messages.put(Request.put(key, rev, body, callback));
		} catch (InterruptedException e) {
			callback.error(key, e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void get(String key, Get callback) {
		try {
			messages.put(Request.get(key, callback));
		} catch (InterruptedException e) {
			callback.error(key, e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(String key, Delete callback) {
		try {
			messages.put(Request.delete(key, callback));
		} catch (InterruptedException e) {
			callback.error(key, e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void list(List callback) {
		try {
			messages.put(Request.list(callback));
		} catch (InterruptedException e) {
			callback.error(e);
			throw new RuntimeException(e);
		}
	}
	
	private static class Request<T> {

		private final String key;
		private final int rev;
		private final byte[] body;
		private final T callback;

		private Request(String key, int rev, byte[] body, T callback) {
			this.key = key;
			this.rev = rev;
			this.body = body;
			this.callback = callback;
		}

		public static Request<Get> get(String key, Get cb) {
			return new Request<Get>(key, 0, null, cb);
		}

		public static Request<Delete> delete(String key, Delete callback) {
			return new Request<Delete>(key, 0, null, callback);
		}

		public static Request<List> list(List callback) {
			return new Request<List>(null, 0, null, callback);
		}

		public static Request<?> put(String key, int rev, byte[] body, Put cb) {
			return new Request<Put>(key, rev, body, cb);
		}

		public void dispatch(Db db) {
			
			if (callback instanceof Put) {
				db.put(key, rev, body, (Put)callback);
			}
			
			else if (callback instanceof Get) {
				db.get(key, (Get)callback);
			}
			
			else if (callback instanceof Delete) {
				db.delete(key, (Delete)callback);
			}
			
			else if (callback instanceof List) {
				db.list((List)callback);
			}
		}
		
	}

	@Override
	public void setAutoPurge(boolean autoPurge) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getRevisions(String key, GetRevisions callback) {
		// TODO Auto-generated method stub
		
	}

}
