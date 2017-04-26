package it.unimi.di.big.mg4j.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.AbstractReference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ReferenceArraySet;
import it.unimi.dsi.fastutil.objects.ReferenceCollection;
import it.unimi.dsi.fastutil.objects.ReferenceCollections;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.util.Map;
import java.util.NoSuchElementException;

/** A simple, brute-force implementation of a fixed-size map from {@linkplain Index indices}
 * to {@linkplain IntervalIterator interval iterators} based on two parallel backing arrays. */

public final class Index2IntervalIteratorMap extends AbstractReference2ReferenceMap<Index, IntervalIterator> {
	private static final long serialVersionUID = 1L;

	/** The keys (valid up to {@link #size}, excluded). */
	protected Index[] key;

	/** The values (parallel to {@link #key}). */
	protected IntervalIterator[] value;

	/** The number of valid entries in {@link #key} and {@link #value}. */
	protected int size;

	public Index2IntervalIteratorMap( final int capacity ) {
		key = new Index[ capacity ];
		value = new IntervalIterator[ capacity ];
	}

	/**
	 * Creates a new empty array map copying the entries of a given map.
	 * 
	 * @param m a map.
	 */
	public Index2IntervalIteratorMap( final Index2IntervalIteratorMap m ) {
		this( m.size() );
		putAll( m );
	}

	private final class EntrySet extends AbstractObjectSet<Reference2ReferenceMap.Entry<Index, IntervalIterator>> {
		@Override
		public ObjectIterator<Reference2ReferenceMap.Entry<Index, IntervalIterator>> iterator() {
			return new AbstractObjectIterator<Reference2ReferenceMap.Entry<Index, IntervalIterator>>() {
				int next = 0;

				public boolean hasNext() {
					return next < size;
				}

				public Entry<Index, IntervalIterator> next() {
					if ( !hasNext() ) throw new NoSuchElementException();
					return new AbstractReference2ReferenceMap.BasicEntry<Index, IntervalIterator>( key[ next ], value[ next++ ] );
				}
			};
		}

		public int size() {
			return size;
		}

		@SuppressWarnings("rawtypes")
		public boolean contains( Object o ) {
			if ( !( o instanceof Map.Entry ) ) return false;
			Map.Entry e = (Map.Entry)o;
			return Index2IntervalIteratorMap.this.containsKey( e.getKey() ) && ( ( Index2IntervalIteratorMap.this.get( e.getKey() ) ) == ( e.getValue() ) );
		}
	}

	@Override
	public ObjectSet<Reference2ReferenceMap.Entry<Index, IntervalIterator>> reference2ReferenceEntrySet() {
		return new EntrySet();
	}

	private int findKey( final Object k ) {
		final Index[] key = this.key;
		for ( int i = size; i-- != 0; ) if ( key[ i ] == k ) return i;
		return -1;
	}

	public IntervalIterator get( final Object k ) {
		final Index[] key = this.key;
		for ( int i = size; i-- != 0; ) if ( key[ i ] == k ) return value[ i ];
		return null;
	}

	public int size() {
		return size;
	}

	@Override
	public void clear() {
		size = 0;
	}

	@Override
	public boolean containsKey( final Object k ) {
		return findKey( k ) != -1;
	}

	@Override
	public boolean containsValue( Object v ) {
		for ( int i = size; i-- != 0; )	if ( value[ i ] == v ) return true;
		return false;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	@Override
	public IntervalIterator put( final Index k, final IntervalIterator v ) {
		final int oldKey = findKey( k );
		if ( oldKey != -1 ) {
			final IntervalIterator oldValue = value[ oldKey ];
			value[ oldKey ] = v;
			return oldValue;
		}
		key[ size ] = k;
		value[ size ] = v;
		size++;
		return null;
	}

	/** A fast version of {{@link #put(Index, IntervalIterator)} that does not return the previous value.
	 * 
	 * @param k the key.
	 * @param v the value.
	 */
	public void add( final Index k, final IntervalIterator v ) {
		final int oldKey = findKey( k );
		if ( oldKey != -1 ) value[ oldKey ] = v;
		else {
			key[ size ] = k;
			value[ size ] = v;
			size++;
		}
	}

	@Override
	public IntervalIterator remove( final Object k ) {
		final int oldPos = findKey( k );
		if ( oldPos == -1 ) return defRetValue;
		final IntervalIterator oldValue = value[ oldPos ];
		final int tail = size - oldPos - 1;
		for ( int i = 0; i < tail; i++ ) {
			key[ oldPos + i ] = key[ oldPos + i + 1 ];
			value[ oldPos + i ] = value[ oldPos + i + 1 ];
		}
		size--;
		key[ size ] = null;
		value[ size ] = null;
		return oldValue;
	}

	@Override
	public ReferenceSet<Index> keySet() {
		return new ReferenceArraySet<Index>( key, size );
	}

	@Override
	public ReferenceCollection<IntervalIterator> values() {
		return ReferenceCollections.unmodifiable( new ReferenceArraySet<IntervalIterator>( value, size ) );
	}
	
	@Override
	public void defaultReturnValue( IntervalIterator unused ) {
		throw new UnsupportedOperationException();
	}
}
