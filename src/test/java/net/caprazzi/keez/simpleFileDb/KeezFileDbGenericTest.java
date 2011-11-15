package net.caprazzi.keez.simpleFileDb;

import java.io.File;

import org.junit.Before;

import net.caprazzi.keez.KeezTest;
import net.caprazzi.keez.onfile.KeezOnFile;

public class KeezFileDbGenericTest extends KeezTest {

	@Before
	public void setUp() {
		// TODO: see http://java.dzone.com/news/in-memory-virtual-filesystems to setup virtual fs
		File testDir = KeezFileDbTest.createTempDir();
		testDir.mkdir();
		db = new KeezOnFile(testDir.getAbsolutePath(), "pfx", false);
	}
	
}
