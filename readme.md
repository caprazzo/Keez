Keez - Naive Java Embeddable Key Value Store
============================================

This is a simple embeddable java key-value store that I use it when 
I need something simple for a prototype or a cheap service.

Api "Features":
 
* async/callback semantics
* optimistic locking (using revision numbers)   

The only provided implementation 'KeezFileDb' is cheap and dirty and works.

For docs, see src/main/java/net/caprazzi/keez/Keez.java

Example usage
=============

```java

	// FROM src/main/java/net/caprazzi/keez/Example.java
	
	Keez.Db db = new KeezFileDb(".", "kz");		
		
	// try get an key that does not exist		
	db.get("somekey", new Get() {

		public void notFound(String key) {
			System.out.println(key + " not found");
		}
		
		public void found(String key, int rev, byte[] data) {}

		@Override
		public void error(String key, Exception e) {
			System.out.println("error while creating " + key);
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
	});
	
	// update a key (notice rev 1)
	byte[] betterdata = "betterdata".getBytes();
	db.put("somekey", 1, betterdata, new Put() {
		
		public void ok(String key, int rev) {
			System.out.println(key + " succesfully updated at rev" + rev);
		}

		public void collision(String key, int yourRev, int foundRev) {}
	});
	
	// list all keys
	db.list(new List() {
		@Override
		public void entries(Iterable<Entry> entries) {
			for(Keez.Entry e : entries) {
				System.out.println("Found " + e.getKey() + "@" + e.getRevision() + ": " + new String(e.getData()));
			}
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
	db.delete("somekey", new Delete() {

		@Override
		public void notFound(String key) {
			System.out.println("Key Not found: " + key);
		}
		
		@Override
		public void deleted(String key, byte[] data) {}
		
	});		
```

## For developers

#### Deploy to local maven repo
mvn -DaltDeploymentRepository=snapshot-repo::default::file:../mcaprari-maven-repo/releases/ clean deploy