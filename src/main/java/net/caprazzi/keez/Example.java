package net.caprazzi.keez;

import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.Put;
import net.caprazzi.keez.simpleFileDb.KeezFileDb;

public class Example {
		
	public static void main(String[] args) {
		
		Keez.Db db = new KeezFileDb(".", "kz");		
		
		// try get an key that does not exist		
		db.get("somekey", new Get() {

			public void notFound(String key) {
				System.out.println(key + " not found");
			}
			
			public void found(String key, int rev, byte[] data) {}
		});
		
		// put a key
		byte[] data = "somedata".getBytes();
		db.put("somekey", 0, data, new Put() {
			
			public void ok(String key) {
				System.out.println(key + " succesfully created");
			}

			public void collision(String key, int yourRev, int foundRev) {}
		});
		
		// read back the key
		db.get("somekey", new Get() {
			
			public void found(String key, int rev, byte[] data) {
				System.out.println("Found data for key [" + key +"] at rev " + rev + ": " + new String(data));
			}
			
			public void notFound(String key) {}			
		});
		
		// update a key (notice rev 1)
		byte[] betterdata = "betterdata".getBytes();
		db.put("somekey", 1, betterdata, new Put() {
			
			public void ok(String key) {
				System.out.println(key + " succesfully updated");
			}

			public void collision(String key, int yourRev, int foundRev) {}
		});
		
		// get a collision
		byte[] evenbetterdata = "evenbetterdata".getBytes();
		db.put("somekey", 1, evenbetterdata, new Put() {
			
			public void collision(String key, int yourRev, int foundRev) {
				System.out.println("Collision while trying to update key [" + key + "] at revision " + yourRev + ": key is at revision " + foundRev);
			}
			
			public void ok(String key) {
				System.out.println(key + " succesfully updated");
			}
			
		});
		
		// delete data
		db.delete("somekey", new Delete() {

			@Override
			public void deleted(String key, byte[] data) {
				System.out.println("Deleted key ["+key+"] with data " + new String(data));
			}

			@Override
			public void notFound(String key) {}
			
		});
		
		// try to delete again
		// delete data
		db.delete("somekey", new Delete() {


			@Override
			public void notFound(String key) {
				System.out.println("Key Not found: " + key);
			}
			
			@Override
			public void deleted(String key, byte[] data) {}
			
		});		
	
		
	}
}
