package net.caprazzi.keez;

import net.caprazzi.keez.Keez.Callback;
import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Entry;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.GetRevisions;
import net.caprazzi.keez.Keez.List;
import net.caprazzi.keez.Keez.Put;

public class Helpers {
	
	public static final void ok(Put callback, String key, int revision) {
		try {
			callback.ok(key, revision);
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}
	
	public static final void applicationError(Callback callback, Exception ex) {
		try {
			callback.applicationError(ex);
		}
		catch (Exception e) {
			// TODO: log this
			e.printStackTrace();
		}
	}

	public static final void collision(Put callback, String key, int yourRev, int foundRev) {
		try {
			callback.collision(key, yourRev, foundRev);
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
		
	}
	
	public static final void found(Get callback, String key, int revision, byte[] body) {
		try {
			callback.found(key, revision, body);
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}
	
	public static final void notFound(Get callback, String key) {
		try {
			callback.notFound(key);
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}
	
	public static final void deleted(Delete callback, String key, byte[] body) {
		try {
			callback.deleted(key, body);
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}

	public static final void notFound(Delete callback, String key) {
		try {
			callback.notFound(key);
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}
	
	public static final void entries(List callback, Iterable<Entry> entries) {
		try {
			callback.entries(entries);
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}

	public static final void notFound(List callback) {
		try {
			callback.notFound();
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}
	
	public static final void found(GetRevisions callback, String key, Iterable<Entry> entries) {
		try {
			callback.found(key, entries);
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}

	public static final void notFound(GetRevisions callback, String key) {
		try {
			callback.notFound(key);
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}	
	
	public static final void notNull(Object o) {
		if (o == null)
			throw new NullPointerException();
	}

	public static void error(Put callback, String key, Exception e) {
		try {
			callback.error(key, e);			
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}

	public static void error(Get callback, String key, Exception e) {
		try {
			callback.error(key, e);			
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}

	public static void error(Delete callback, String key, Exception e) {
		try {
			callback.error(key, e);			
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}

	public static void error(List callback, Exception e) {
		try {
			callback.error(e);			
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}

	public static void error(GetRevisions callback, String key, Exception e) {
		try {
			callback.error(key, e);			
		}
		catch (Exception ex) {
			applicationError(callback, ex);
		}
	}
	
}
