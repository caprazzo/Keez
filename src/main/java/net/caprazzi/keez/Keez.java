package net.caprazzi.keez;


/**
 * Simple embedded key-value store API.
 * 
 * The API supports 4 operations (put, get, delete, list).
 * 
 * Put operations require to specify a revision number
 * 
 * All operations accept a callback object that is used to
 * return data and results to the caller.
 * 
 * @see Example.java for usage
 * 
 */
public class Keez {

	public interface Db {
			
		/**
		 * If set to true, the implementation should automatically
		 * remove old revisions when a key is updated
		 * @param autoPurge
		 */
		void setAutoPurge(boolean autoPurge);
		
		/**
		 * Create or Update a key. 
		 * New keys must have rev=0, while updates must have the same
		 * revision number as the newest update in the database. 
		 * At each update the revision number is increased by 1. 
		 * 
		 * callback.ok is invoked on a successful put
		 * callback.collision in invoked if
		 * 	- the revision number is not 0 and the key does not exist
		 *  - the revision number is different from the newest rev in the db
		 * callback.error is called on any other error
		 * 
		 * @param key
		 * @param rev revision to update. 
		 * 			Should be 0 or match the highest revision number in the db.
		 * @param body
		 * @param callback
		 */
		public void put(String key, int rev, byte[] body, Put callback);

		/**
		 * Get value.
		 * The latest revision of a key is always returned.
		 * 
		 * callback.found is invoked if the key is found
		 * callback.notFound is invoked if the key is not found
		 * callback.error is invoked on any other error
		 * 
		 * @param key
		 * @param callback
		 */
		public void get(String key, Get callback);

		/**
		 * Delete a key. Deletes all revisions.
		 * 
		 * @param key
		 * @param callback
		 */
		public void delete(String key, Delete callback);

		/**
		 * List all keys in the database.
		 * 
		 * callback.entries is invoked even if the database is empty
		 * callback.error is called in case of any exception
		 * 
		 * @param list
		 */
		public void list(List callback);

	}

	public static abstract class Get {
		/**
		 * Invoked on Get success
		 * 
		 * @param key
		 * @param rev current revision in the database
		 * @param data
		 */
		public abstract void found(String key, int rev, byte[] data);

		/**
		 * Invoked when key is not found
		 * 
		 * @param key
		 */
		public abstract void notFound(String key);
				
		/**
		 * Invoked on errors but not on "not found"
		 * 
		 * @param key
		 * @param e
		 */
		public abstract void error(String key, Exception e);
	}

	public static abstract class Put {
		/**
		 * Invoked when put completed succesfully
		 * 
		 * @param key
		 */
		public abstract void ok(String key, int rev);

		/**
		 * Invoked when there is a collision in the key revision
		 * 
		 * @param key
		 * @param yourRev
		 *            revision this call tried to update
		 * @param foundRev
		 *            revision found in db
		 */
		public abstract void collision(String key, int yourRev, int foundRev);

		/**
		 * Invoked on any error
		 * 
		 * @param key
		 * @param e
		 *            underlying exception
		 */
		public abstract void error(String key, Exception e);

	}

	public static abstract class Delete {

		/**
		 * Invoked when delete completed succesfully
		 * 
		 * @param key
		 * @param data
		 *            the data contained in the key before deletion
		 */
		public abstract void deleted(String key, byte[] data);

		/**
		 * Invoked when key not found
		 * 
		 * @param key
		 */
		public abstract void notFound(String key);

		/**
		 * Invoked on any error (not on not found)
		 * 
		 * @param key
		 * @param e
		 *            underlying exception
		 */
		public abstract void error(String key, Exception e);

	}
	
	public static abstract class List {

		public abstract void entries(Iterable<Entry> entries);

		public abstract void error(Exception ex);
	}
	
	public static class Entry {

		private final String key;
		private final int revision;
		private final byte[] data;

		public Entry(String key, int revision, byte[] data) {
			this.key = key;
			this.revision = revision;
			this.data = data;
		}

		public byte[] getData() {
			return data;
		}

		public String getKey() {
			return key;
		}

		public int getRevision() {
			return revision;
		}
		
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return super.toString() + "::" + key + "@" + revision;
		}
		
	}

}
