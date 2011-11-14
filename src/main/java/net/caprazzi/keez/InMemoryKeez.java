package net.caprazzi.keez;

import java.util.Collection;

import net.caprazzi.keez.Keez.Delete;
import net.caprazzi.keez.Keez.Get;
import net.caprazzi.keez.Keez.List;
import net.caprazzi.keez.Keez.Put;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class InMemoryKeez implements Keez.Db {

	private final Multimap<String, Value> data;
	
	public InMemoryKeez() {
		Multimap<String, Value> data = HashMultimap.create();
		this.data = Multimaps.synchronizedMultimap(data);
	}
	
	@Override
	public void put(String key, int rev, byte[] body, Put callback) {
		if (rev == 0) {
			create(key, body, callback);
			return;
		}
		
	}

	private void create(String key, byte[] body, Put callback) {
		Collection<Value> revisions = data.get(key);
		if (revisions != null) {
			int foundRev = getRevision(revisions);
			callback.collision(key, 0, foundRev);
			return;
		}
		
		data.put(key, new Value(0, body));
		callback.ok(key, 1);		
	}

	@Override
	public void get(String key, Get callback) {
	}

	@Override
	public void delete(String key, Delete callback) {
	}

	@Override
	public void list(List callback) {
		
	}
	
	private static class Value {
		private final int rev;
		private final byte[] body;

		public Value(int rev, byte[] body) {
			this.rev = rev;
			this.body = body;
		}
	}

}
