package net.caprazzi.keez.inmemory;

import java.util.SortedSet;

import net.caprazzi.keez.Keez;
import net.caprazzi.keez.Keez.Db;
import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Entry;
import net.caprazzi.keez.Keez.Get;
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
	public void put(String key, int rev, byte[] body, Put callback) {
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
				callback.collision(key, rev, lastRevision);
				return;
			}
			
			int newRevision = lastRevision + 1;
			
			data.put(key, new Value(newRevision, body));
			
			if (autoPurge) {
				purgeOldRevisions(key, newRevision);
			}
		
			callback.ok(key, newRevision);
			
		}
		catch (Exception e) {
			callback.error(key, e);
		}
		
	}
	
	@Override
	public void get(String key, Get callback) {
		try {
			SortedSet<Value> revisions = data.get(key);
			if (revisions.size() == 0) {
				callback.notFound(key);
				return;
			}
			
			Value value = revisions.last();
			callback.found(key, value.rev, value.body);
		}
		catch (Exception e) {
			callback.error(key, e);
		}
	}

	@Override
	public void delete(String key, Delete callback) {
		try {
			SortedSet<Value> revisions = data.get(key);
			if (revisions.size() == 0) {
				callback.notFound(key);
				return;
			}
			
			Value value = revisions.last();
			data.removeAll(key);
			callback.deleted(key, value.body);
		}
		catch (Exception e) {
			callback.error(key, e);
		}
	}

	@Override
	public void list(List callback) {
		try {
			callback.entries(Iterables.transform(data.keySet(), new Function<String, Entry>() {
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

	private void purgeOldRevisions(String key, int newRevision) {
		SortedSet<Value> revisions = data.get(key);
		SortedSet<Value> toRemove = revisions.subSet(revisions.first(), revisions.last());
		revisions.removeAll(toRemove);
	}

	private void create(String key, byte[] body, Put callback) {
		SortedSet<Value> revisions = data.get(key);
		if (revisions.size() > 0) {
			int foundRev = getRevision(revisions);
			callback.collision(key, 0, foundRev);
			return;
		}
		
		Value v = new Value(1, body);
		data.put(key, v);
		callback.ok(key, v.rev);		
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
