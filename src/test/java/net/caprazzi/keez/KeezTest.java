package net.caprazzi.keez;

import static org.junit.Assert.*;
import net.caprazzi.keez.Keez.Db;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.Put;

import org.junit.Before;
import org.junit.Test;

/**
 * Abstract test class meant to be subclassed to test specific implementations
 * Create the Db instance in a @Before methos in the subclass
 */
public abstract class KeezTest {
	
	protected Db db;
	protected boolean flag = false;
	
	@Before
	public void setup() {
		flag = false;
		System.out.println("Setup in KeezTest");
	}

	@Test
	public void test_should_put_and_get_key() {
		db.put("foo", 0, "data".getBytes(), new PutTestHelp() {
			@Override
			public void ok(String key, int revision) {
				db.get("foo", new GetTestHelp() {
					@Override
					public void found(String key, int rev, byte[] data) {
						assertEquals("foo", key);
						assertEquals(1, rev);
						assertEquals("data", new String(data));
						flag = true;
					}
				});
			}
		});
		assertTrue(flag);
	}
	
	@Test
	public void test_should_get_last_revision() {
		db.put("akey", 0, "data".getBytes(), PutOk);
		db.put("akey", 1, "new-data".getBytes(), PutOk);
		db.put("akey", 2, "more-new-data".getBytes(), PutOk);
		
		db.get("akey", new GetTestHelp() {
			@Override
			public void found(String key, int rev, byte[] data) {
				assertEquals("akey", key);
				assertEquals(3, rev);
				assertEquals("more-new-data", new String(data));
				flag = true;
			}
		});
		assertTrue(flag);
	}
	
	@Test
	public void test_should_fail_to_get_key_if_not_exists() {
		db.get("foo", new GetTestHelp() {
			@Override
			public void notFound(String key) {
				assertEquals("foo", key);
				flag = true;
			}
		});
		assertTrue(flag);
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
			throw new RuntimeException("unexpected found");
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
	
	private static final Put PutOk = new PutTestHelp() {
		@Override public void ok(String key, int revision) {}
	};
	
}
