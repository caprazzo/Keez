package net.caprazzi.keez;

import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Entry;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.List;
import net.caprazzi.keez.Keez.Put;
import net.caprazzi.keez.onfile.KeezOnFile;

public class Example {
		
	public static void main(String[] args) {
		
		//Keez.Db db = new InMemoryKeez();		
		Keez.Db db = new KeezOnFile(".", "kz", false);
		
		// try get an key that does not exist		
		db.get("somekey", new Get() {

			public void notFound(String key) {
				System.out.println(key + " not found");
			}
			
			public void found(String key, int rev, byte[] data) {}

			@Override
			public void error(String key, Exception e) {
				System.out.println("error while getting " + key);
				e.printStackTrace();
			}
			
		});
		
		// put a key
		byte[] data = "somedata".getBytes();
		db.put("somekey", 0, data, new Put() {
			
			public void ok(String key, int rev) {
				System.out.println(key + " succesfully created at rev " + rev);
			}

			public void collision(String key, int yourRev, int foundRev) {}

			@Override
			public void error(String key, Exception e) {
			}
		});
		
		// read back the key
		db.get("somekey", new Get() {
			
			public void found(String key, int rev, byte[] data) {
				System.out.println("Found data for key [" + key +"] at rev " + rev + ": " + new String(data));
			}
			
			public void notFound(String key) {}		
			
			@Override
			public void error(String key, Exception e) {
				System.out.println("error while creating " + key);
				e.printStackTrace();
			}
		});
		
		// put another key
		byte[] moredata = "moredata".getBytes();
		db.put("someotherkey", 0, moredata, new Put() {
			
			public void ok(String key, int rev) {
				System.out.println(key + " succesfully created at rev " + rev);
			}

			public void collision(String key, int yourRev, int foundRev) {}

			@Override
			public void error(String key, Exception e) {
				// TODO Auto-generated method stub
				
			}
		});
		
		// update a key (notice rev 1)
		byte[] betterdata = "betterdata".getBytes();
		db.put("somekey", 1, betterdata, new Put() {
			
			public void ok(String key, int rev) {
				System.out.println(key + " succesfully updated at rev" + rev);
			}

			public void collision(String key, int yourRev, int foundRev) {}

			@Override
			public void error(String key, Exception e) {
				// TODO Auto-generated method stub
				
			}
		});
		
		// list all keys
		db.list(new List() {
			@Override
			public void entries(Iterable<Entry> entries) {
				for(Keez.Entry e : entries) {
					System.out.println("Found " + e.getKey() + "@" + e.getRevision() + ": " + new String(e.getData()));
				}
			}

			@Override
			public void error(Exception ex) {
				// TODO Auto-generated method stub
				
			}
		});
		
		// get a collision
		byte[] evenbetterdata = "evenbetterdata".getBytes();
		db.put("somekey", 1, evenbetterdata, new Put() {
			
			public void collision(String key, int yourRev, int foundRev) {
				System.out.println("Collision while trying to update key [" + key + "] at revision " + yourRev + ": key is at revision " + foundRev);
			}
			
			public void ok(String key, int rev) {
				System.out.println(key + " succesfully updated at rev " + rev);
			}

			@Override
			public void error(String key, Exception e) {
				// TODO Auto-generated method stub
				
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

			@Override
			public void error(String key, Exception e) {
				// TODO Auto-generated method stub
				
			}
			
		});
		
		// try to delete again
		db.delete("somekey", new Delete() {

			@Override
			public void notFound(String key) {
				System.out.println("Key Not found: " + key);
			}
			
			@Override
			public void deleted(String key, byte[] data) {}

			@Override
			public void error(String key, Exception e) {
				// TODO Auto-generated method stub
				
			}
			
		});		
	
		
	}
}
