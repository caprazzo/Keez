package net.caprazzi.keez.inmemory;

import static net.caprazzi.keez.Helpers.collision;
import static net.caprazzi.keez.Helpers.deleted;
import static net.caprazzi.keez.Helpers.entries;
import static net.caprazzi.keez.Helpers.found;
import static net.caprazzi.keez.Helpers.notFound;
import static net.caprazzi.keez.Helpers.ok;
import static net.caprazzi.keez.Helpers.notNull;

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
			
			SortedSet<Value> revisions = data.get(key);
			if (revisions.size() == 0) {
				callback.collision(key, rev, -1);
				return;
			}
			
			int lastRevision = getRevision(revisions);
			
			if (rev != lastRevision) {
				collision(callback, key, rev, lastRevision);
				return;
			}
			
			int newRevision = lastRevision + 1;
			
			data.put(key, new Value(newRevision, body));
			
			if (autoPurge) {
				purgeOldRevisions(key, newRevision);
			}
		
			ok(callback, key, newRevision);
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
			SortedSet<Value> revisions = data.get(key);
			if (revisions.size() == 0) {
				notFound(callback, key);
				return;
			}
			
			Value value = revisions.last();
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
			SortedSet<Value> revisions = data.get(key);
			if (revisions.size() == 0) {
				notFound(callback, key);
				return;
			}
			
			Value value = revisions.last();
			data.removeAll(key);
			deleted(callback, key, value.body);
		}
		catch (Exception e) {
			callback.error(key, e);
		}
	}
	
	@Override
	public void list(List callback) {
		notNull(callback);
		
		try {
			Set<String> keys = data.keySet();
			if (keys.size() == 0) {
				notFound(callback);
				return;
			}
			entries(callback, Iterables.transform(keys, new Function<String, Entry>() {
				@Override
				public Entry apply(String key) {
					Value v = data.get(key).last();
					return new Entry(key, v.rev, v.body);
				}
			}));
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
			SortedSet<Value> revisions = data.get(key);
			if (revisions.size() == 0) {
				notFound(callback, key);
				return;
			}
			
			found(callback, key, Iterables.transform(revisions, new Function<Value, Entry>() {
				@Override
				public Entry apply(Value value) {
					return new Entry(key, value.rev, value.body);
				}
			}));
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

	private void create(String key, byte[] body, Put callback) {
		SortedSet<Value> revisions = data.get(key);
		if (revisions.size() > 0) {
			int foundRev = getRevision(revisions);
			collision(callback, key, 0, foundRev);
			return;
		}
		
		Value v = new Value(1, body);
		data.put(key, v);
		int revision = v.rev;
		
		ok(callback, key, revision);		
	}
	
	private int getRevision(SortedSet<Value> revisions) {
		return revisions.last().rev;		
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
