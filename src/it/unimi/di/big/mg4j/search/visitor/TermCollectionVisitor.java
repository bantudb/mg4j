package it.unimi.di.big.mg4j.search.visitor;

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
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Reference2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2IntMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A visitor collecting information about terms appearing
 * in a {@link it.unimi.di.big.mg4j.search.DocumentIterator}.
 * 
 * <P>The purpose of this visitor is that of exploring before iteration the structure
 * of a {@link DocumentIterator} to count how many terms are actually used, and set up some
 * appearing in all leaves of nonzero frequency (the latter
 * condition is used to skip empty iterators), possibly {@linkplain #prepare(ReferenceSet) considering
 * just a subset of indices}. For this visitor to work, all leaves
 * of nonzero frequency must return a non-<code>null</code> value on 
 * a call to {@link it.unimi.di.big.mg4j.index.IndexIterator#term()}.
 * 
 * <p>During the visit, we keep track of which index/term pair have been already
 * seen. Each pair is assigned an distinct <em>offset</em>&mdash;a number between
 * zero and the overall number of distinct pairs&mdash;which is stored into
 * each index iterator {@linkplain it.unimi.di.big.mg4j.index.IndexIterator#id() id}
 * and is used afterwards to access quickly data about the pair. Note that duplicate index/term pairs
 * get the same offset. The overall number of distinct pairs is returned
 * by {@link #numberOfPairs()} after a visit. 
 * 
 * <p>The indices appearing in some valid pair are recorded; they are accessible as a vector returned 
 * by {@link #indices()}, and the map from positions in this vector to indices
 * is inverted by {@link #indexMap()}. 
 * 
 * <p>If you need to fix the index map, there's a special {@link #prepare(ReferenceSet)} method.
 * In that case <strong>only terms associated with indices in the provided set will be
 * collected</strong>.
 * 
 * <p><strong>Warning</strong>: the semantics of {@link #prepare(ReferenceSet)} described above has 
 * been implemented in MG4J 4.0. Previously, the effect of {@link #prepare(ReferenceSet)} was
 * just that of adding artificially indices to the index set.
 * 
 * <p>The offset assigned to each pair index/term
 * is returned by {@link #offset(Index, String)}. Should you need to know the terms
 * associated with each index, they are returned by {@link #terms(Index)}.
 * 
 * <p>After a term collection, usually counters are set 
 * up by a visit of {@link it.unimi.di.big.mg4j.search.visitor.CounterSetupVisitor}.
 */

public class TermCollectionVisitor extends AbstractDocumentIteratorVisitor {
	private final static Logger LOGGER = LoggerFactory.getLogger( TermCollectionVisitor.class );
	private final static boolean DEBUG = false;

	/** The map from indices to maps from terms to offsets. The map themselves are linked,
	 * so terms are always returned in the same order (the visit order). */
	private final Reference2ObjectMap<Index,Object2IntMap<String>> index2termMap;
	/** A map from indices to positions in {@link #index}. */
	private final Reference2IntMap<Index> indexMap;
	/** A map from terms (indistinctly belonging to some index) to term ids. */
	private final Object2IntLinkedOpenHashMap<String> term2Id;
	/** The overall number of pairs index/term. */
	private int numberOfPairs;
	/** The array of indices involved in this query, returned by {@link #indices()}. */ 
	private Index[] index;
	/** Whether the set of allowed indices has been fixed by {@link #prepare(ReferenceSet)}. */
	private boolean rigid;
	
	/** Creates a new term-collection visitor. */
	
	public TermCollectionVisitor() {
		index2termMap = new Reference2ObjectOpenHashMap<Index,Object2IntMap<String>>();
		indexMap = new Reference2IntLinkedOpenHashMap<Index>( Hash.DEFAULT_INITIAL_SIZE, .5f );
		term2Id = new Object2IntLinkedOpenHashMap<String>();
		term2Id.defaultReturnValue( -1 );
	}
	
	private void reset() {
		index = null;
		index2termMap.clear();
		indexMap.clear();
		term2Id.clear();
		numberOfPairs = 0;
	}

	/** Prepares this term-collection visitor.
	 * 
	 * @return this term-collection visitor.
	 */
	public TermCollectionVisitor prepare() {
		rigid = false;
		reset();
		return this;
	}
	
	/** Prepares this term-collection visitor, possibly specifying the indices that should be collected.
	 * 
	 * @param indices the set of indices that will be collected; if empty, the all indices will be collected
	 * (e.g., the call is equivalent to {@link #prepare()}).
	 * @return this term-collection visitor.
	 */
	public TermCollectionVisitor prepare( final ReferenceSet<Index> indices ) {
		if ( indices.isEmpty() ) return prepare();
		reset();

		// If indices is not empty, we do eager instantiation of the index set. This must be kept in sync with the lazy part.
		int c = 0;
		for( final Index i: indices ) {
			indexMap.put( i, c++ );
			final Object2IntMap<String> termMap = new Object2IntLinkedOpenHashMap<String>( Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR );
			index2termMap.put( i, termMap );
			termMap.defaultReturnValue( -1 );
		}
		return this;
	}

	
	public Boolean visit( final IndexIterator indexIterator ) throws IOException {
		// We skip empty iterators, indices without counts and, in the rigid case, indices not belonging to the index map.
		if ( indexIterator.frequency() > 0 && indexIterator.index().hasCounts && ( ! rigid || indexMap.containsKey( indexIterator.index() ) ) ) {  
			final Index index = indexIterator.index();
			final String term = indexIterator.term();
			
			if ( term == null ) throw new NullPointerException( "This visitor needs a non-null term for each index iterator of nonzero frequency" );
			
			if ( ! term2Id.containsKey( term ) ) term2Id.put( term, term2Id.size() );
			
			if ( DEBUG ) LOGGER.debug( "Visiting leaf: index=" + index + ", term=" + term );
			
			final Object2IntMap<String> termMap;

			if ( ! indexMap.containsKey( index ) ) {
				// This index has never been seen before
				indexMap.put( index, indexMap.size() );
				// Lazy instantiation of the term map. Please keep in sync with eager instantiation in prepare(ReferenceSet).
				index2termMap.put( index, termMap = new Object2IntLinkedOpenHashMap<String>( Hash.DEFAULT_INITIAL_SIZE, .5f ) );
				termMap.defaultReturnValue( -1 );
			}
			else termMap = index2termMap.get( index );
			
			int offset = termMap.getInt( term );
			if ( offset == -1 ) termMap.put( term, offset = numberOfPairs++ ); // Unknown index/term pair 
			indexIterator.id( offset );
			if ( DEBUG ) LOGGER.debug( "Offset for index iterator " + indexIterator + ": " + offset );
		}
		else indexIterator.id( -1 ); // Unused pairs are marked with -1.
		return Boolean.TRUE; 
	}

	/** Returns the number of distinct index/term pair corresponding to 
	 * nonzero-frequency index iterators in the last visit.
	 * 
	 * @return the number distinct index/term pair corresponding to 
	 * nonzero-frequency index iterators.
	 */
	public int numberOfPairs() {
		return numberOfPairs;
	}
	
	/** Returns the indices met during pair collection.
	 * 
	 * <p>Note that the returned array does not include indices only associated
	 * to index iterators of zero frequency, unless {@link #prepare(ReferenceSet)} was
	 * called with a nonempty argument.
	 * 
	 * @return the indices met during term collection.
	 */
	public Index[] indices() {
		if ( index == null ) index = indexMap.keySet().toArray( new Index[ index2termMap.size() ] );
		return index;
	}
	
	/** Returns a map from indices met during term collection to their position
	 * into {@link #indices()}.
	 * 
	 * <p>Note that the returned map does not include as keys indices only associated
	 * to index iterators of zero frequency, unless {@link #prepare(ReferenceSet)} was
	 * called with a nonempty argument.
	 * 
	 * @return a map from indices met during term collection to their position
	 * into {@link #indices()}.
	 */
	public Reference2IntMap<Index> indexMap() {
		return indexMap;
	}
	
	/** Returns the terms associated with the given index.
	 * 
	 * @param index an index.
	 * @return the terms associated with <code>index</code>, in the same order in which
	 * they appeared during the visit, skipping duplicates, if some nonzero-frequency iterator
	 * based on <code>index</code> was found; <code>null</code> otherwise.
	 */
	public String[] terms( final Index index ) {
		final Object2IntMap<String> termMap = index2termMap.get( index );
		return termMap == null ? null : termMap.keySet().toArray( new String[ termMap.size() ] );
	}
	
	/** Returns the a map associating terms appearing in the query with ids.
	 * 
	 * @return a map from terms appearing in the query (in indices with counts) to ids.
	 */
	public Object2IntLinkedOpenHashMap<String> term2Id() {
		return term2Id;
	}
	
	/** Returns the offset associated with a given pair index/term.
	 * 
	 * @param index an index appearing in {@link #indices()}.
	 * @param term a term appearing in the array returned by {@link #terms(Index)} with argument <code>index</code>. 
	 * @return the offset associated with the pair <code>index</code>/<code>term</code>.
	 */

	public int offset( final Index index, final String term ) {
		return index2termMap.get( index ).getInt( term );
	}

	public String toString() {
		return "[Leaves: " + numberOfPairs + "; " + index2termMap + "]";
	}
}
