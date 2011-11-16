package net.caprazzi.keez.inmemory;

import static net.caprazzi.keez.Helpers.collision;
import static net.caprazzi.keez.Helpers.deleted;
import static net.caprazzi.keez.Helpers.entries;
import static net.caprazzi.keez.Helpers.found;
import static net.caprazzi.keez.Helpers.notFound;
import static net.caprazzi.keez.Helpers.ok;
import static net.caprazzi.keez.Helpers.notNull;

import java.util.LinkedList;
import java.util.Set;
import java.util.SortedSet;

import net.caprazzi.keez.Keez;
import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Entry;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.GetRevisions;
import net.caprazzi.keez.Keez.List;
import net.caprazzi.keez.Keez.Put;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

public class KeezInMemory implements Keez.Db {
	
	private SortedSetMultimap<String, Value> data;
	private boolean autoPurge;

	public KeezInMemory() {
		SortedSetMultimap<String, Value> data = TreeMultimap.create();
		this.data = Multimaps.synchronizedSortedSetMultimap(data);
	}
	
	@Override
	public void setAutoPurge(boolean autoPurge) {
		this.autoPurge = autoPurge;
	}
	
	@Override
	public void put(String key, int rev, byte[] body, Put callback) {
		notNull(key);
		notNull(body);
		notNull(callback);
		
		try {
			if (rev == 0) {
				create(key, body, callback);
				return;
			}
			
			Value value = null;
			Value lastRevision = null;
			synchronized (data) {
				lastRevision = getLastRevision(key);
				if (lastRevision != null && rev == lastRevision.rev) {
					int newRevision = lastRevision.rev + 1;	
					value = new Value(newRevision, body);
					data.put(key, value);			
					if (autoPurge) {
						purgeOldRevisions(key, newRevision);
					}
				}
			}
			
			if (value != null) {
				ok(callback, key, value.rev);
				return;
			}
			
			if (lastRevision != null) {
				collision(callback, key, rev, lastRevision.rev);
			}
			else {
				collision(callback, key, rev, -1);
			}
		}
		catch (Exception e) {
			callback.error(key, e);
		}
	}
	
	@Override
	public void get(String key, Get callback) {
		notNull(key);
		notNull(callback);
		
		try {
			Value value = getLastRevision(key);
			if (value == null) {
				notFound(callback, key);
				return;
			}
			
			int revision = value.rev;
			byte[] body = value.body;
			found(callback, key, revision, body);		
		}
		catch (Exception e) {
			callback.error(key, e);
		}
	}

	@Override
	public void delete(String key, Delete callback) {
		notNull(key);
		notNull(callback);
		
		try {
			
			Value value = null;
			SortedSet<Value> revisions = data.get(key);
			synchronized (data) {
				if (revisions.size() > 0) {
					value = revisions.last();
					data.removeAll(key);
				}
			}
			
			if (value == null) {
				notFound(callback, key);
			}
			else {
				deleted(callback, key, value.body);	
			}			
		}
		catch (Exception e) {
			callback.error(key, e);
		}
	}
	
	@Override
	public void list(List callback) {
		notNull(callback);
		
		try {			
			LinkedList<Entry> entries = new LinkedList<Keez.Entry>();
			Set<String> keys = data.keySet();
			synchronized (data) {
				for(String key : keys) {
					Value v = data.get(key).last();
					entries.add(new Entry(key, v.rev, v.body));
				}
			}
			
			if (entries.size() == 0) {
				notFound(callback);
				return;
			}
			
			entries(callback, entries);
		}
		catch(Exception e) {
			callback.error(e);
		}
	}
	
	@Override
	public void getRevisions(final String key, GetRevisions callback) {
		notNull(key);
		notNull(callback);
		
		try {
			LinkedList<Entry> entries = new LinkedList<Keez.Entry>();
			SortedSet<Value> revisions = data.get(key);
			synchronized (data) {
				for(Value v : revisions) {
					entries.add(new Entry(key, v.rev, v.body));
				}
			}
			
			if (entries.size() == 0) {
				notFound(callback, key);
				return;
			}
			
			found(callback, key, entries);
		}
		catch(Exception e) {
			callback.error(key, e);
		}
	}

	private void purgeOldRevisions(String key, int newRevision) {
		SortedSet<Value> revisions = data.get(key);
		SortedSet<Value> toRemove = revisions.subSet(revisions.first(), revisions.last());
		revisions.removeAll(toRemove);
	}
	
	/**
	 * Puts key:value if the key does not exist, otherwise returns the latest revision for that key
	 * @param key
	 * @param value
	 * @return the latest revision for this key if the key is already present, otherwise null
	 */
	private Value putIfNotFound(String key, Value value) {
		synchronized (data) {			
			if (data.containsKey(key)) {
				return data.get(key).last();
			}	
			data.put(key, value);
			return null;
		}
	}

	private void create(String key, byte[] body, Put callback) {
		Value value = new Value(1, body);
		Value found = putIfNotFound(key, value);
		if (found == null) {
			ok(callback, key, value.rev);
		}
		else {
			collision(callback, key, 0, found.rev);
		}		
	}
	
	private Value getLastRevision(String key) {
		synchronized (data) {
			SortedSet<Value> revisions = data.get(key);
			if (revisions.size() > 0) {
				return revisions.last();
			}			
		}
		return null;
	}

	private static class Value  implements Comparable<Value> {
		private final int rev;
		private final byte[] body;

		public Value(int rev, byte[] body) {
			this.rev = rev;
			this.body = body;
		}

		@Override
		public int compareTo(Value other) {
			return rev - other.rev;
		}
	}

}
