package net.caprazzi.keez.simpleFileDb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.Put;
import net.caprazzi.keez.simpleFileDb.KeezFileDb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KeezFileDbTest {

	File testDir;
	private KeezFileDb db;
	private byte[] data = new byte[] { 'a','b','c' };
	
	@Before
	public void setUp() {
		testDir = createTempDir();
		db = new KeezFileDb(testDir.getAbsolutePath(), "pfx");
	}

	@After
	public void tearDown() {
		testDir.delete();
	}	

	@Test
	public void put_should_create_file_on_ok() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.put("newKey", 0, data, new PutTestHelp() {			
			public void ok(String key) {
				assertEquals("newKey", key);
				flag.set(true);
			}
		});		
		assertTrue(flag.get());
		assertTrue(findFile(testDir, "pfx-newKey.1"));		
	}
	
	@Test
	public void put_should_create_one_file_for_each_revision() {
		db.put("key", 0, data, PutNoop);
		assertTrue(findFile(testDir, "pfx-key.1"));
		
		db.put("key", 1, data, PutNoop);
		assertTrue(findFile(testDir, "pfx-key.2"));
		
		db.put("key", 2, data, PutNoop);
		assertTrue(findFile(testDir, "pfx-key.3"));
	}
	
	@Test
	public void put_should_collide_if_key_exists() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.put("key", 0, data, PutNoop);
		db.put("key", 0, data, new PutTestHelp() {
			@Override
			public void collision(String key, int yourRev, int foundRev) {
				assertEquals("key", key);
				assertEquals(0, yourRev);
				assertEquals(1, foundRev);
				flag.set(true);
			}
		});
		assertTrue(flag.get());
	}
	
	@Test
	public void put_should_collide_if_rev_not_zero_on_create() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.put("newKey", 1, data, new PutTestHelp() {			
			public void collision(String key, int yourRev, int foundRev) {
				assertEquals("newKey", key);
				assertEquals(1, yourRev);
				assertEquals(-1, foundRev);
				flag.set(true);
			}
		});
		assertTrue(flag.get());
	}
	
	@Test
	public void put_should_collide_if_put_with_old_rev() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.put("newKey", 0, data, PutNoop);		
		db.put("newKey", 1, data, PutNoop);		
		db.put("newKey", 1, data, new PutTestHelp() {
			public void collision(String key, int yourRev, int nextRev) {
				assertEquals("newKey", key);
				assertEquals(1, yourRev);
				assertEquals(2, nextRev);
				flag.set(true);
			}			
		});
		assertTrue(flag.get());
	}
	
	@Test
	public void put_should_collide_if_put_with_too_new_rev() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.put("newKey", 0, data, PutNoop);	
		db.put("newKey", 1, data, PutNoop);
		db.put("newKey", 3, data, new PutTestHelp() {
			public void collision(String key, int yourRev, int foundRev) {
				assertEquals("newKey", key);
				assertEquals(3, yourRev);
				assertEquals(2, foundRev);
				flag.set(true);
			}			
		});
		assertTrue(flag.get());
	}
	
	@Test
	public void get_should_invoke_not_found_on_no_key() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.get("somekey", new GetTestHelp() {
			public void notFound(String key) {
				assertEquals("somekey", key);
				flag.set(true);
			}
		});
		assertTrue(flag.get());
	}
	
	@Test
	public void get_should_get_value_if_key_exists() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.put("somekey", 0, data, PutNoop);		
		db.get("somekey", new GetTestHelp() {
			public void found(String key, int rev, byte[] foundData) {
				assertEquals(1, rev);
				assertEquals("somekey", key);
				assertTrue(Arrays.equals(data, foundData));
				flag.set(true);				
			}
		});
		
		assertTrue(flag.get());
	}
	
	@Test
	public void should_get_latest_revision_if_key_exists() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.put("somekey", 0, data, PutNoop);
		db.put("somekey", 1, "otherdata".getBytes(), PutNoop);
		db.put("somekey", 2, "latest".getBytes(), PutNoop);
		
		db.get("somekey", new GetTestHelp() {
			public void found(String key, int rev, byte[] foundData) {
				assertEquals(3, rev);
				assertEquals("somekey", key);
				assertTrue(Arrays.equals("latest".getBytes(), foundData));
				flag.set(true);				
			}
		});		
		assertTrue(flag.get());
	}
	
	@Test
	public void delete_should_get_not_found_if_no_key() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.delete("somekey", new Delete() {
			public void deleted(String key, byte[] data) {
			}
			public void notFound(String key) {
				assertEquals("somekey", key);
				flag.set(true);
			}
		});
		assertTrue(flag.get());
	}
	
	@Test
	public void delete_should_remove_all_files() {
		db.put("key", 0, data, PutNoop);
		db.put("key", 1, data, PutNoop);
		db.put("key", 2, data, PutNoop);
		
		assertTrue(findFile(testDir, "pfx-key.1"));
		assertTrue(findFile(testDir, "pfx-key.2"));
		assertTrue(findFile(testDir, "pfx-key.3"));	
		
		db.delete("key", DeleteNoop);		
		
		assertFalse(findFile(testDir, "pfx-key.1"));
		assertFalse(findFile(testDir, "pfx-key.2"));
		assertFalse(findFile(testDir, "pfx-key.3"));
	}
	
	
	@Test
	public void delete_should_return_latest_data_before_delete() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.put("key", 0, data, PutNoop);
		db.put("key", 1, "newData".getBytes(), PutNoop);
		db.put("key", 2, "latestData".getBytes(), PutNoop);
		
		db.delete("key", new DeleteTestHelp() {
			@Override
			public void deleted(String key, byte[] data) {
				assertTrue(Arrays.equals("latestData".getBytes(), data));
				flag.set(true);
			}
		});
		assertTrue(flag.get());
	}
	
	@Test
	public void should_error_if_bad_char_in_key() {
		final AtomicBoolean flag = new AtomicBoolean();
		db.put("  ", 0, data, new PutTestHelp() {
			public void error(String key, Exception e) {
				assertTrue(e.getMessage().contains("invalid character in key"));
				flag.set(true);
			}
		});
		assertTrue(flag.get());
		
		flag.set(false);
		db.put("/", 1, data, new PutTestHelp() {
			public void error(String key, Exception e) {
				assertTrue(e.getMessage().contains("invalid character in key"));
				flag.set(true);
			}
		});
		assertTrue(flag.get());
		
		flag.set(false);
		db.get("/", new GetTestHelp() {
			public void error(String key, Exception e) {
				assertTrue(e.getMessage().contains("invalid character in key"));
				flag.set(true);
			}
		});
		assertTrue(flag.get());
		
		flag.set(false);
		db.delete("/", new DeleteTestHelp() {
			public void error(String key, Exception e) {
				assertTrue(e.getMessage().contains("invalid character in key"));
				flag.set(true);
			}
		});
		assertTrue(flag.get());
	}
	
	//////// TEST UTILS BELOW ////////////
	
	private boolean findFile(File dir, final String expected) {
		File[] files = dir.listFiles(new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		        return name.equals(expected); 
		    }
		});
		return (files != null && files.length == 1);
	}

	// temp dire creation code from guava
	private static final int TEMP_DIR_ATTEMPTS = 10000;
	public static File createTempDir() {
		File baseDir = new File(System.getProperty("java.io.tmpdir"));
		String baseName = System.currentTimeMillis() + "-";

		for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
			File tempDir = new File(baseDir, baseName + counter);
			if (tempDir.mkdir()) {
				return tempDir;
			}
		}
		throw new IllegalStateException("Failed to create directory within "
				+ TEMP_DIR_ATTEMPTS + " attempts (tried " + baseName + "0 to "
				+ baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
	}
	
	private static final Put PutNoop = new Put() {
		@Override public void ok(String key) {}
		@Override public void collision(String key, int yourRev, int foundRev) {}
	}; 
	
	private static final Delete DeleteNoop = new Delete() {
		@Override public void deleted(String key, byte[] data) {}
		@Override public void notFound(String key) {}
	};
	
	public static class PutTestHelp extends Put {

		@Override
		public void ok(String key) {
			throw new RuntimeException("unexpected put success");
		}

		@Override
		public void collision(String key, int yourRev, int foundRev) {
			throw new RuntimeException("unexpected put collision");			
		}
		
		@Override
		public void error(String key, Exception e) {
			throw new RuntimeException("unexpected put error", e);
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
	
}
