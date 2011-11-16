package net.caprazzi.keez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import net.caprazzi.keez.Keez.Db;
import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Entry;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.GetRevisions;
import net.caprazzi.keez.Keez.List;
import net.caprazzi.keez.Keez.Put;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

/**
 * Abstract test class meant to be sub-classed to test specific implementations
 * Create the Db instance in a @Before methods in the subclass
 */
public abstract class KeezTest {
	
	protected Db db;
	protected boolean called = false;
	
	@Before
	public void setup() {
		called = false;
		System.out.println("Setup in KeezTest");
	}

	@Test public void put_should_put_and_get_key() {
		db.put("foo", 0, "data".getBytes(), PutOk);
		db.get("foo", new GetTestHelp() {
			@Override
			public void found(String key, int rev, byte[] data) {
				assertEquals("foo", key);
				assertEquals(1, rev);
				assertEquals("data", new String(data));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void put_should_collide_if_creating_with_non_zero_rev() {
		db.put("akey", 1, "data".getBytes(), new PutTestHelp() {
			@Override
			public void collision(String key, int yourRev, int foundRev) {
				assertEquals("akey", key);
				assertEquals(1, yourRev);
				assertEquals(-1, foundRev);
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void put_should_collide_if_putting_zero_twice() {
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 0, "data".getBytes(), new PutTestHelp() {
			@Override
			public void collision(String key, int yourRev, int foundRev) {
				assertEquals("akey", key);
				assertEquals(0, yourRev);
				assertEquals(1, foundRev);
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void put_should_collide_if_putting_revision_twice() {
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 1, "data".getBytes(), PutOk);
		db.put("akey", 1, "data".getBytes(), new PutTestHelp() {
			@Override
			public void collision(String key, int yourRev, int foundRev) {
				assertEquals("akey", key);
				assertEquals(1, yourRev);
				assertEquals(2, foundRev);
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void put_should_collide_if_skipping_revision_ahead() {
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 1, "data".getBytes(), PutOk);
		db.put("akey", 3, "data".getBytes(), new PutTestHelp() {
			@Override
			public void collision(String key, int yourRev, int foundRev) {
				assertEquals("akey", key);
				assertEquals(3, yourRev);
				assertEquals(2, foundRev);
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void put_should_collide_if_creating_twice() {
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 0, "data".getBytes(), new PutTestHelp() {
			@Override
			public void collision(String key, int yourRev, int foundRev) {
				assertEquals("akey", key);
				assertEquals(0, yourRev);
				assertEquals(1, foundRev);
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void put_should_purge_old_revisions_if_enabled() {
		db.setAutoPurge(true);
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 1, "data".getBytes(), PutOk);
		db.put("akey", 2, "data".getBytes(), PutOk);
		db.put("akey", 3, "data".getBytes(), PutOk);
		
		db.getRevisions("akey", new GetRevisionsTestHelp() {
			@Override
			public void found(String key, Iterable<Entry> revisions) {
				assertEquals("akey", key);
				assertEquals(1, Iterators.size(revisions.iterator()));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void put_should_not_purge_old_revisions_if_enabled() {
		db.setAutoPurge(false);
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 1, "data".getBytes(), PutOk);
		db.put("akey", 2, "data".getBytes(), PutOk);
		db.put("akey", 3, "data".getBytes(), PutOk);
		
		db.getRevisions("akey", new GetRevisionsTestHelp() {
			@Override
			public void found(String key, Iterable<Entry> revisions) {
				assertEquals("akey", key);
				assertEquals(4, Iterators.size(revisions.iterator()));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void put_create_should_bounce_exceptions_in_ok() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.put("akey", 0, "data".getBytes(), new PutTestHelp() {
			
			@Override
			public void ok(String key, int revision) {				
				throw new RuntimeException("kaboom");
			}
			
			@Override
			public void error(String key, Exception e) {
				error_called.set(true);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals(ex.getMessage(), "kaboom");
				called = true;
				throw new RuntimeException("BOOM");
			}
		});
		
		assertFalse(error_called.get());
		assertTrue(called);
	}
	
	@Test public void put_update_should_bounce_exceptions_thrown_in_ok() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 1, "moredata".getBytes(), new PutTestHelp() {
			
			@Override
			public void ok(String key, int revision) {
				throw new RuntimeException("kaboom");
			}
			
			@Override
			public void error(String key, Exception e) {
				error_called.set(true);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals(ex.getMessage(), "kaboom");
				called = true;
				throw new RuntimeException("BOOM");
			}
		});
		
		assertTrue(called);
		assertFalse(error_called.get());
	}
	
	@Test public void put_should_bounce_exceptions_thrown_in_error() {
		//TODO: How to best force error() in a sane way?		
	}
	
	@Test public void put_create_should_bounce_exceptions_thrown_in_collision() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 0, "moredata".getBytes(), new PutTestHelp() {
			
			@Override
			public void collision(String key, int yourRev, int foundRev) {
				throw new RuntimeException("kaboom");
			}
			
			@Override
			public void error(String key, Exception e) {
				error_called.set(true);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals(ex.getMessage(), "kaboom");
				called = true;
				throw new RuntimeException("BOOM");
			}
		});
		
		assertTrue(called);
		assertFalse(error_called.get());
	}
	
	@Test public void put_update_should_bounce_exceptions_thrown_in_collision() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 1, "data".getBytes(), PutOk);
		db.put("akey", 1, "moredata".getBytes(), new PutTestHelp() {
			
			@Override
			public void collision(String key, int yourRev, int foundRev) {
				throw new RuntimeException("kaboom");
			}
			
			@Override
			public void error(String key, Exception e) {
				error_called.set(true);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals(ex.getMessage(), "kaboom");
				called = true;
				throw new RuntimeException("BOOM");
			}
		});
		
		assertTrue(called);
		assertFalse(error_called.get());
	}
	
	@Test public void get_should_get_only_revision() {
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.get("akey", new GetTestHelp() {
			@Override
			public void found(String key, int rev, byte[] data) {
				assertEquals("akey", key);
				assertEquals(1, rev);
				assertEquals("data", new String(data));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void get_should_get_last_revision() {
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 1, "new-data".getBytes(), PutOk);
		db.put("akey", 2, "more-new-data".getBytes(), PutOk);
		
		db.get("akey", new GetTestHelp() {
			@Override
			public void found(String key, int rev, byte[] data) {
				assertEquals("akey", key);
				assertEquals(3, rev);
				assertEquals("more-new-data", new String(data));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void get_should_call_not_found_if_not_exists() {
		db.get("foo", new GetTestHelp() {
			@Override
			public void notFound(String key) {
				assertEquals("foo", key);
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void get_should_bounce_exceptions_thrown_in_found() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.put("xxxx", 0, "data".getBytes(), PutOk);
		db.get("xxxx", new GetTestHelp() {

			@Override
			public void found(String key, int rev, byte[] data) {
				throw new RuntimeException("EXCEPTION IN FOUND");
			}

			@Override
			public void error(String key, Exception e) {
				error_called.set(true);			
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals("EXCEPTION IN FOUND", ex.getMessage());
				called = true;
				throw new RuntimeException(ex);
			}
		});
		
		assertTrue(called);
		assertFalse(error_called.get());
	}
	
	@Test public void get_should_bounce_exceptions_thrown_in_not_found() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.get("xxxx", new GetTestHelp() {

			@Override
			public void notFound(String key) {
				throw new RuntimeException("EXCEPTION IN NOTFOUND");
			}

			@Override
			public void error(String key, Exception e) {
				error_called.set(true);			
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals("EXCEPTION IN NOTFOUND", ex.getMessage());
				called = true;
				throw new RuntimeException("EXCEPTION IN ERROR HANDLER");
			}
		});
		
		assertTrue(called);
		assertFalse(error_called.get());
	}
	
	@Test public void get_should_bounce_exceptions_thrown_in_error() {
		//TODO: how do I force an error() in a sane way?
	}
	
	@Test public void get_revisions_should_call_not_found_if_no_key() {
		db.getRevisions("xxxx", new GetRevisionsTestHelp() {
			@Override
			public void notFound(String key) {
				assertEquals("xxxx", key);
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void get_revisions_should_get_one_revision() {
		db.put("xxxx", 0, "data".getBytes(), PutOk);
		db.getRevisions("xxxx", new GetRevisionsTestHelp() {
			@Override
			public void found(String key, Iterable<Entry> revisions) {
				assertEquals("xxxx", key);
				Entry[] array = Iterables.toArray(revisions, Entry.class);
				assertEquals(1, array.length);
				assertEquals(1, array[0].getRevision());				
				assertEquals("xxxx", array[0].getKey());
				assertEquals("data", new String(array[0].getData()));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void get_revisions_should_get_all_revisions() {
		db.put("xxxx", 0, "data-0".getBytes(), PutOk);
		db.put("xxxx", 1, "data-1".getBytes(), PutOk);
		db.put("xxxx", 2, "data-2".getBytes(), PutOk);
		db.getRevisions("xxxx", new GetRevisionsTestHelp() {
			@Override
			public void found(String key, Iterable<Entry> revisions) {
				assertEquals("xxxx", key);
				Entry[] array = Iterables.toArray(revisions, Entry.class);
				assertEquals(3, array.length);
				
				assertEquals(1, array[0].getRevision());				
				assertEquals("xxxx", array[0].getKey());
				assertEquals("data-0", new String(array[0].getData()));
				
				assertEquals(2, array[1].getRevision());				
				assertEquals("xxxx", array[1].getKey());
				assertEquals("data-1", new String(array[1].getData()));
				
				assertEquals(3, array[2].getRevision());				
				assertEquals("xxxx", array[2].getKey());
				assertEquals("data-2", new String(array[2].getData()));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void get_revisions_should_get_one_revision_if_auto_purge() {
		db.setAutoPurge(true);
		db.put("xxxx", 0, "data-0".getBytes(), PutOk);
		db.put("xxxx", 1, "data-1".getBytes(), PutOk);
		db.put("xxxx", 2, "data-2".getBytes(), PutOk);
		db.getRevisions("xxxx", new GetRevisionsTestHelp() {
			@Override
			public void found(String key, Iterable<Entry> revisions) {
				assertEquals("xxxx", key);
				Entry[] array = Iterables.toArray(revisions, Entry.class);
				assertEquals(1, array.length);				
				
				assertEquals(3, array[0].getRevision());				
				assertEquals("xxxx", array[0].getKey());
				assertEquals("data-2", new String(array[0].getData()));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void get_revisions_should_bounce_exceptions_thrown_in_found() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.put("xxxx", 0, "data-0".getBytes(), PutOk);
		db.getRevisions("xxxx", new GetRevisionsTestHelp() {
			
			@Override
			public void found(String key, Iterable<Entry> revisions) {
				throw new RuntimeException("EXCEPTION IN FOUND");
			}
			
			@Override
			public void error(String key, Exception e) {
				error_called.set(true);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals("EXCEPTION IN FOUND", ex.getMessage());
				called = true;
				throw new RuntimeException("EXCEPTION IN APP ERROR");
			}
			
		});
		
		assertFalse(error_called.get());
		assertTrue(called);
	}
	
	@Test public void get_revisions_should_bounce_exceptions_thrown_in_not_found() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.getRevisions("xxxx", new GetRevisionsTestHelp() {
			
			@Override
			public void notFound(String key) {
				throw new RuntimeException("EXCEPTION IN FOUND");
			}
			
			@Override
			public void error(String key, Exception e) {
				error_called.set(true);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals("EXCEPTION IN FOUND", ex.getMessage());
				called = true;
				throw new RuntimeException("EXCEPTION IN APP ERROR");
			}
			
		});
		
		assertFalse(error_called.get());
		assertTrue(called);
	}
	
	@Test public void get_revisions_should_bounce_exceptions_thrown_in_error() {
		//TODO: How to best force error() in a sane way?
	}	
	
	@Test public void delete_should_call_not_found_if_no_key() {
		db.delete("xxxx", new DeleteTestHelp() {
			@Override
			public void notFound(String key) {
				assertEquals("xxxx", key);
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void delete_should_call_deleted_on_success() {
		db.put("xxxx", 0, "data".getBytes(), PutOk);
		db.delete("xxxx", new DeleteTestHelp() {
			@Override
			public void deleted(String key, byte[] data) {
				assertEquals("xxxx", key);
				assertEquals("data", new String(data));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void delete_should_remove_one_revision() {
		db.put("xxxx", 0, "data".getBytes(), PutOk);
		db.delete("xxxx", DeleteOk);
		db.get("xxxx", new GetTestHelp() {
			@Override
			public void notFound(String key) {
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void delete_should_remove_all_revision() {
		db.put("xxxx", 0, "data".getBytes(), PutOk);
		db.put("xxxx", 1, "data".getBytes(), PutOk);
		db.put("xxxx", 2, "data".getBytes(), PutOk);
		db.delete("xxxx", DeleteOk);
		db.get("xxxx", new GetTestHelp() {
			@Override
			public void notFound(String key) {
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void delete_should_bounce_exceptions_thrown_in_deleted() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.put("xxxx", 0, "data".getBytes(), PutOk);
		db.delete("xxxx", new DeleteTestHelp() {
			@Override
			public void deleted(String key, byte[] data) {
				throw new RuntimeException("EXCEPTION IN DELETE");
			}
			
			@Override
			public void error(String key, Exception e) {
				error_called.set(false);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals("EXCEPTION IN DELETE", ex.getMessage());
				called = true;
				throw new RuntimeException("EXCEPTION IN ERROR HANDLER");
			}
		});
		
		assertTrue(called);
		assertFalse(error_called.get());
	}
	
	@Test public void delete_should_bounce_exceptions_thrown_in_not_found() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.delete("xxxx", new DeleteTestHelp() {
			@Override
			public void notFound(String key) {
				throw new RuntimeException("EXCEPTION IN NOTFOUND");
			}
			
			@Override
			public void error(String key, Exception e) {
				error_called.set(false);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals("EXCEPTION IN NOTFOUND", ex.getMessage());
				called = true;
				throw new RuntimeException("EXCEPTION IN ERROR HANDLER");
			}
		});
		
		assertTrue(called);
		assertFalse(error_called.get());
	}
	
	@Test public void delete_should_bounce_exceptions_thrown_in_error() {
		// TODO: how to force error() in a sane way?
	}
	
	@Test public void list_should_call_not_found_if_empty_database() {
		db.list(new ListTestHelp() {
			public void notFound() {
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void list_should_find_the_last_revision_for_all_keys() {
		db.put("keyA", 0, "data-A-0".getBytes(), PutOk);
		db.put("keyA", 1, "data-A-1".getBytes(), PutOk);
		db.put("keyB", 0, "data-B-0".getBytes(), PutOk);		
		db.list(new ListTestHelp() {
			@Override
			public void entries(Iterable<Entry> entries) {
				Entry[] array = Iterables.toArray(entries, Entry.class);
				assertEquals(2, array.length);
				Entry keyA = array[0];
				assertEquals("keyA", keyA.getKey());
				assertEquals(2, keyA.getRevision());
				assertEquals("data-A-1", new String(keyA.getData()));
				
				Entry keyB = array[1];
				assertEquals("keyB", keyB.getKey());
				assertEquals(1, keyB.getRevision());
				assertEquals("data-B-0", new String(keyB.getData()));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void list_should_not_see_deleted_entries() {
		db.put("keyA", 0, "data-A-0".getBytes(), PutOk);
		db.put("keyA", 1, "data-A-1".getBytes(), PutOk);
		db.put("keyB", 0, "data-B-0".getBytes(), PutOk);
		
		db.delete("keyA", DeleteOk);
		
		db.list(new ListTestHelp() {
			@Override
			public void entries(Iterable<Entry> entries) {
				Entry[] array = Iterables.toArray(entries, Entry.class);
				assertEquals(1, array.length);
				Entry keyB = array[0];
				assertEquals("keyB", keyB.getKey());
				assertEquals(1, keyB.getRevision());
				assertEquals("data-B-0", new String(keyB.getData()));
				called = true;
			}
		});
		
		assertTrue(called);
	}
	
	@Test public void list_should_bounce_exceptions_thrown_in_not_found() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.list(new ListTestHelp() {
			@Override
			public void notFound() {
				throw new RuntimeException("EXCEPTION IN NOTFOUND");
			}
			
			@Override
			public void error(Exception e) {
				error_called.set(false);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals("EXCEPTION IN NOTFOUND", ex.getMessage());
				called = true;
				throw new RuntimeException("EXCEPTION IN ERROR HANDLER");
			}
		});
		
		assertTrue(called);
		assertFalse(error_called.get());
	}
	
	@Test public void list_should_bounce_exceptions_thrown_in_entries() {
		final AtomicBoolean error_called = new AtomicBoolean(false);
		db.put("keyA", 0, "data-A-0".getBytes(), PutOk);
		db.put("keyB", 0, "data-B-0".getBytes(), PutOk);
		db.list(new ListTestHelp() {
			@Override
			public void entries(Iterable<Entry> entries) {
				throw new RuntimeException("EXCEPTION IN ENTRIES");
			}
			
			@Override
			public void error(Exception e) {
				error_called.set(false);
			}
			
			@Override
			public void applicationError(Exception ex) {
				assertEquals("EXCEPTION IN ENTRIES", ex.getMessage());
				called = true;
				throw new RuntimeException("EXCEPTION IN ERROR HANDLER");
			}
		});
		
		assertTrue(called);
		assertFalse(error_called.get());
	}
	
	@Test public void list_should_bounce_exceptions_thrown_in_error() {
		// TODO: force error() to test this
	}
	
	public static class PutTestHelp extends Put {

		@Override
		public void ok(String key, int revision) {
			throw new RuntimeException("unexpected put success");
		}

		@Override
		public void collision(String key, int yourRev, int foundRev) {
			throw new RuntimeException("unexpected put collision key:" + key + " yourRev:" + yourRev + " foundRev:" + foundRev);			
		}
		
		@Override
		public void error(String key, Exception e) {
			throw new RuntimeException("unexpected put error", e);
		}
		
	}
	
	public static class GetTestHelp extends Get {

		@Override
		public void found(String key, int rev, byte[] data) {
			throw new RuntimeException("unexpected found key:" + key + " rev:" + rev);
		}

		@Override
		public void notFound(String key) {
			throw new RuntimeException("unexpected not found " + key);
		}
		
		@Override
		public void error(String key, Exception e) {
			throw new RuntimeException("unexpected error", e);
		}
		
	}
	
	public static class DeleteTestHelp extends Delete {

		@Override
		public void deleted(String key, byte[] data) {
			throw new RuntimeException("unexpected delete success");
		}

		@Override
		public void notFound(String key) {
			throw new RuntimeException("unexpected not found");			
		}
		
		@Override
		public void error(String key, Exception e) {
			throw new RuntimeException("unexpected error", e);
		}		
	}
	
	public static class GetRevisionsTestHelp extends GetRevisions {

		@Override
		public void error(String key, Exception e) {
			throw new RuntimeException("unexpected error", e);
		}

		@Override
		public void found(String key, Iterable<Entry> revisions) {
			throw new RuntimeException("unexpected found key:" + key);
		}

		@Override
		public void notFound(String key) {
			throw new RuntimeException("unexpected not found " + key);			
		}
		
	}
	
	public static class ListTestHelp extends List {
		@Override
		public void entries(Iterable<Entry> entries) {
			throw new RuntimeException("unexpected entries call");
		}

		@Override
		public void error(Exception ex) {
			throw new RuntimeException("unexpected error call", ex);			
		}

		@Override
		public void notFound() {
			throw new RuntimeException("unexpected not found");	
		}
	}
	
	protected static final Put PutOk = new PutTestHelp() {
		@Override public void ok(String key, int revision) {}
	};
	
	private static final Delete DeleteOk = new DeleteTestHelp() {
		@Override public void deleted(String key, byte[] data) {};
	};
	
	protected static final Get GetOk = new GetTestHelp() {
		@Override public void found(String key, int rev, byte[] data) {};
	};
	
	protected static final Get GetNoop = new GetTestHelp() {
		@Override public void found(String key, int rev, byte[] data) {};
		public void notFound(String key) {};
	};
	
}
