package net.caprazzi.keez;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import net.caprazzi.keez.inmemory.KeezInMemory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class KeezConcurrencyTest {

	KeezInMemory db;
	final byte[] data = "data".getBytes();
	ExecutorService executor = Executors.newFixedThreadPool(2);

	@Before public void setup() {
		db = new KeezInMemory();
	}
	
	@After
	public void tearDown() {
		executor.shutdownNow();
	}
	
	 public void concurrent_writes() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(3);
		final AtomicInteger count = new AtomicInteger(1);
		
		Task put = new Task() {
			public void task() throws Exception {
				int num = count.getAndIncrement();
				System.out.println(num);
				int k = 100000;
				for (int i=0; i<k; i++) {
					db.put(Integer.toString(k*num + i), 0, data, KeezTest.PutOk);
				}
				latch.countDown();
			}
		};
		
		executor.invokeAll(Arrays.asList(new Task[] { put, put, put }));		
		latch.await();
	}
	
	@Test public void concurrent_reads_and_writes() throws InterruptedException {
		final int count = 100000;
		Task put = new Task() {
			public void task() throws Exception {
				for (int i=0; i<count; i++) {
					db.put(Integer.toString(i), 0, data, KeezTest.PutOk);
				}		
			}
		};
		
		Task get = new Task() {
			public void task() throws Exception {
				for (int i=0; i<count; i++) {
					db.get(Integer.toString(i), KeezTest.GetNoop);
				}
			}
		};
	
		invokeAndWait(put, get);
		
		System.out.println("done");
	}
	
	@Test public void all_operations() throws InterruptedException {
		int k = 10;
		final String[] keys = new String[10];
		for (int i=0; i<k; i++)
			keys[i] = Integer.toString(i);
				
		Task task = new Task() {
			public void task() throws Exception {
				for (int i=0; i< 1000; i++) {
					for (String key : keys) {
						db.put(key, 0, data, new KeezTest.PutTestHelp() {
							public void collision(String key, int yourRev, int foundRev) {
								db.put(key, foundRev, data, KeezTest.PutOk);
							};
							public void ok(String key, int revision) {};
						});
					}
					System.out.print(".");
				}
			}
		};
		
		invokeAndWait(task, task, task, task);
	}
	
	private void invokeAndWait(Task... tasks) throws InterruptedException {
		List<Future<String>> invokeAll = executor.invokeAll(Arrays.asList(tasks));
		while(!Iterators.all(invokeAll.iterator(), new Predicate<Future<String>>() {
			@Override
			public boolean apply(Future input) {
				return input.isDone();
			}
		}));
	}
	
	
	private static abstract class Task implements Callable<String> {
		 @Override
		final public String call() throws Exception {
			task();
			return null;
		}
		 
		 public abstract void task() throws Exception;
	}
}
