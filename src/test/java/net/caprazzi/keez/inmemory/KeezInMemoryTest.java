package net.caprazzi.keez.inmemory;

import org.junit.Before;

import net.caprazzi.keez.KeezTest;

public class KeezInMemoryTest extends KeezTest {

	@Before
	public void setUp() {
		System.out.println("Setup in KeezInMemoryTest");
		this.db = new KeezInMemory();
	}
	
}
