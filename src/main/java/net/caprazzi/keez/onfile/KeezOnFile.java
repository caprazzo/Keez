package net.caprazzi.keez.onfile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;

import net.caprazzi.keez.Keez;
import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.GetRevisions;
import net.caprazzi.keez.Keez.List;
import net.caprazzi.keez.Keez.Put;

import static net.caprazzi.keez.Helpers.collision;
import static net.caprazzi.keez.Helpers.deleted;
import static net.caprazzi.keez.Helpers.entries;
import static net.caprazzi.keez.Helpers.found;
import static net.caprazzi.keez.Helpers.notFound;
import static net.caprazzi.keez.Helpers.ok;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * Naive file-based implementation of Keez
 * 	- values are stored on files
 *  - each key/revision is stored in a file
 *  - all operations are synchronized around an awful global lock
 *  - only alphanum chars are allowed for keys
 *  - despite the async/callback semantics, all methods are blocking
 */
public class KeezOnFile implements Keez.Db {

	private Logger logger = LoggerFactory.getLogger(KeezOnFile.class);
	
	// global lock, defintely too broad
	// could use a more fine grained lock based on the
	// directory or on each file
	private final static Object lock = new Object();
	private final File directory;
	private final String prefix;
	private boolean autoPurge;

	public KeezOnFile(String directory, String prefix, boolean createDir) {
		if (!isValidKey(prefix)) {
			throw new RuntimeException("invalid character in prefix [" + prefix + "]");
		}
		this.directory = new File(directory);
		if (createDir && !this.directory.exists()) {
			this.directory.mkdir();
		}
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
					collision(callback, key, rev, -1);
					return;
				}
				
				int lastRev = getRevision(keyFiles[keyFiles.length-1]);
				if (lastRev != rev) {
					collision(callback, key, rev, lastRev);
					return;
				}
				
				int newRev = lastRev + 1;
												
				File newFile = new File(filePath(key, newRev));
				FileOutputStream writer = new FileOutputStream(newFile);
				writer.write(data);
				writer.close();
				
				if (autoPurge) {
					purgeOldRevisions(key, newRev);
				}

				ok(callback, key, newRev);
			} catch (Exception e) {
				callback.error(key, e);
			}
		}
	}	

	private void purgeOldRevisions(String key, int foundRev) {
		File[] keyFiles = findKeyFile(key);
		for(File file : keyFiles) {
			int revision = getRevision(file);
			if (revision < foundRev) {
				boolean deleted = file.delete();
				if (!deleted) {
					logger.error("could not delete file " + file);
				}
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
					collision(callback, key, 0, foundRev);
					return;
				}
				
				int foundRev = 1;
				
				File newFile = new File(filePath(key, foundRev));
				FileOutputStream writer = new FileOutputStream(newFile);
				writer.write(data);
				writer.close();

				ok(callback, key, foundRev);				
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
				notFound(callback, key);
				return;
			}
			int foundRev = getRevision(keyFiles[keyFiles.length-1]);
			File f = new File(filePath(key, foundRev));
			
			try {
				FileInputStream in = new FileInputStream(f);
				byte[] data = IOUtils.toByteArray(in);
				in.close();
				found(callback, key, foundRev, data);
			} catch (FileNotFoundException ex) {
				notFound(callback, key);
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
				notFound(callback, key);
				return;
			}
			int foundRev = getRevision(keyFiles[keyFiles.length-1]);
			File f = new File(filePath(key, foundRev));
			try {
				FileInputStream in = new FileInputStream(f);
				byte[] data = IOUtils.toByteArray(in);
				in.close();
				
				// delete all key files
				for (File keyFile : keyFiles) {
					if (!keyFile.delete()) {
						callback.error(key, new Exception("Could not delete one of the files for [" + key + "]: " + keyFile 
								+ " exists: " + keyFile.exists() + " w:" + keyFile.canWrite() + " "));
						return;
					}
				}
				deleted(callback, key, data);
			} catch (FileNotFoundException ex) {
				notFound(callback, key);
			} catch (IOException e) {
				callback.error(key, e);
			}
		}
	}
	
	@Override
	public void list(final List callback) {
		HashMap<String, Integer> keys = findLatestRevisions();
		if (keys.size() == 0) {
			notFound(callback);
			return;			
		}
		try {
			Iterable<net.caprazzi.keez.Keez.Entry> entries = Iterables.transform(keys.entrySet(), new Function<Entry<String, Integer>, Keez.Entry>() {
				@Override
				public net.caprazzi.keez.Keez.Entry apply(Entry<String, Integer> e) {
					File f = new File(filePath(e.getKey(), e.getValue()));
					byte[] data;
					try {
						FileInputStream in = new FileInputStream(f);
						data = IOUtils.toByteArray(in);
						in.close();
						return new Keez.Entry(e.getKey(), e.getValue(), data);
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					} 			
				}
			});
			entries(callback, entries);
		}
		catch (Exception e) {
			callback.error(e);
		}
	}
	
	private int getRevision(File file) {
		String[] parts = file.getAbsolutePath().split("\\.");
		return Integer.parseInt(parts[parts.length-1]);
	}

	/**
	 * Find all files for a specific key and return them sorted by revision number ascending
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
		
		Arrays.sort(files, new Comparator<File>() {
			@Override
			public int compare(File fa, File fb) {
				Integer reva = getRevision(fa);
				Integer revb = getRevision(fb);
				return reva.compareTo(revb);
			}
			
		});
		
		return files;
	}
	
	private HashMap<String, Integer> findLatestRevisions() {
		File[] files = listAllFiles();
		HashMap<String,Integer> map = new HashMap<String, Integer>();		
		for(File file : files) {
			Integer rev = getRevision(file);
			String key = getKey(file);
			Integer oldRevision = map.get(key);
			if (oldRevision != null) {
				map.put(key, Math.max(oldRevision, rev));
			}
			else map.put(key, rev);
		}
		return map;
	}
	
	private String getKey(File file) {
		return file.getName().substring(prefix.length() + 1).split("\\.")[0];
	}

	private File[] listAllFiles() {
		return directory.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith(prefix + "-");
			}
		});
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

	@Override
	public void setAutoPurge(boolean autoPurge) {
		this.autoPurge = autoPurge;
	}

	@Override
	public void getRevisions(final String key, GetRevisions callback) {
		try {
			File[] keyFiles = findKeyFile(key);
			if (keyFiles.length == 0) {
				notFound(callback, key);
				return;
			}
			
			Iterable<Keez.Entry> entries = Iterables.transform(Arrays.asList(keyFiles), new Function<File, Keez.Entry>() {
				@Override
				public Keez.Entry apply(File file) {
					int rev = getRevision(file);
					try {
						FileInputStream in = new FileInputStream(file);
						byte[] data = IOUtils.toByteArray(in);
						in.close();
						return new Keez.Entry(key, rev, data);
					} catch (FileNotFoundException e) {
						throw new RuntimeException(e);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}				
			});
			
			found(callback, key, entries);
		}
		catch (Exception ex) {
			callback.error(key, ex);
		}
	}

}
