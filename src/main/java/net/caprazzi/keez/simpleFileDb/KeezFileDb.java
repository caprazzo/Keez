package net.caprazzi.keez.simpleFileDb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;

import net.caprazzi.keez.Keez;

import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.Put;

import org.eclipse.jetty.util.IO;

/**
 * Naive file-based implementation of Keez
 * 	- values are stored on files
 *  - each key/revision is stored in a file
 *  - all operations are synchronized around an awful global lock
 *  - only alphanum chars are allowed for keys
 *  - despite the async/callback semantics, all methods are blocking
 */
public class KeezFileDb implements Keez.Db {

	// global lock, defintely too broad
	// could use a more fine grained lock based on the
	// directory or on the file itself
	private final static Object lock = new Object();
	private final File directory;
	private final String prefix;

	public KeezFileDb(String directory, String prefix) {
		if (!isValidKey(prefix)) {
			throw new RuntimeException("invalid character in prefix [" + prefix + "]");
		}
		this.directory = new File(directory);
		this.prefix = prefix;
	}

	@Override
	public void put(String key, int rev, byte[] data, Put callback) {
		if (rev == 0) {
			create(key, data, callback);
			return;
		}
		
		if (!isValidKey(key)) {
			callback.error(key, new RuntimeException("invalid character in key ["+key+"]"));
			return;
		}

		synchronized (lock) {
			try {
				File[] keyFiles = findKeyFile(key);
				if (keyFiles.length == 0) {
					callback.collision(key, rev, -1);
					return;
				}
				else {					
					int foundRev = getRevision(keyFiles[keyFiles.length-1]);
					if (foundRev != rev) {
						callback.collision(key, rev, foundRev);
						return;
					}
				}
												
				File newFile = new File(filePath(key, rev + 1));
				FileOutputStream writer = new FileOutputStream(newFile);
				writer.write(data);
				writer.close();

				callback.ok(key);
			} catch (Exception e) {
				callback.error(key, e);
			}
		}
	}	

	private void create(String key, byte[] data, Put callback) {
		
		if (!isValidKey(key)) {
			callback.error(key, new RuntimeException("invalid character in key ["+key+"]"));
			return;
		}

		synchronized (lock) {
			try {
				File[] keyFiles = findKeyFile(key);
				
				if (keyFiles.length > 0) {
					int foundRev = getRevision(keyFiles[keyFiles.length-1]);
					callback.collision(key, 0, foundRev);
					return;
				}
				
				File newFile = new File(filePath(key, 1));
				FileOutputStream writer = new FileOutputStream(newFile);
				writer.write(data);
				writer.close();

				callback.ok(key);				
			} catch (Exception e) {
				callback.error(key, e);
			}
		}
	}

	@Override
	public void get(String key, Get callback) {
		
		if (!isValidKey(key)) {
			callback.error(key, new RuntimeException("invalid character in key ["+key+"]"));
			return;
		}
		
		synchronized (lock) {
			File[] keyFiles = findKeyFile(key);
			if (keyFiles.length == 0) {
				callback.notFound(key);
				return;
			}
			int foundRev = getRevision(keyFiles[keyFiles.length-1]);
			File f = new File(filePath(key, foundRev));
			
			try {
				FileInputStream in = new FileInputStream(f);
				byte[] data = IO.readBytes(in);
				in.close();
				callback.found(key, foundRev, data);
			} catch (FileNotFoundException ex) {
				callback.notFound(key);
			} catch (Exception e) {
				callback.error(key, e);
			}
		}
	}

	@Override
	public void delete(String key, Delete callback) {
		
		if (!isValidKey(key)) {
			callback.error(key, new RuntimeException("invalid character in key ["+key+"]"));
			return;
		}
		
		synchronized (lock) {
			File[] keyFiles = findKeyFile(key);
			if (keyFiles.length == 0) {
				callback.notFound(key);
				return;
			}
			int foundRev = getRevision(keyFiles[keyFiles.length-1]);
			File f = new File(filePath(key, foundRev));
			try {
				FileInputStream in = new FileInputStream(f);
				byte[] data = IO.readBytes(in);
				in.close();
				
				// delete all key files
				for (File keyFile : keyFiles) {
					if (!keyFile.delete()) {
						callback.error(key, new Exception("Could not delete one of the files."));
						return;
					}
				}
				callback.deleted(key, data);
			} catch (FileNotFoundException ex) {
				callback.notFound(key);
			} catch (IOException e) {
				callback.error(key, e);
			}
		}
	}
	
	private int getRevision(File file) {
		String[] parts = file.getAbsolutePath().split("\\.");
		return Integer.parseInt(parts[parts.length-1]);
	}

	/**
	 * Find all files for a specific key
	 * 
	 * @param dir
	 * @param key
	 * @return
	 */
	private File[] findKeyFile(final String key) {
		File[] files = directory.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(prefix + "-" + key + ".");
			}
		});
		return files;
	}
	
	/**
	 * Build file path from key and revision
	 * @param key
	 * @param rev
	 * @return
	 */
	private String filePath(String key, int rev) {
		return directory + "/" + prefix + "-" + key + "." + rev;
	}

	private boolean isValidKey(String key) {
		 return key.matches("[A-Za-z0-9]+");
	}

}
