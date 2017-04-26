package it.unimi.di.big.mg4j.search;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2009-2016 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITfNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.query.nodes.Remap;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.ReferenceArraySet;
import it.unimi.dsi.fastutil.objects.ReferenceSet;

import java.io.IOException;
import java.util.Map;

/** A decorator that remaps interval iterator requests.
 * 
 * <P>Sometimes it is necessary to combine with positional operators
 * (e.g., {@link ConsecutiveDocumentIterator}) intervals from different indices.
 * By wrapping with an instance of this class a {@link DocumentIterator}, the requests
 * for intervals will be remapped following a map given at construction time.
 * 
 * <P>This class distinguishes between <em>internal</em> indices, which are those actually
 * provided by the underlying {@link DocumentIterator}, and <em>external</em> indices, which
 * are those exposed by {@link #indices()}, and with which {@link #intervalIterator(Index)} 
 * should be called. 
 * The map provided at construction time should remap external indices to internal
 * indices (note the inversion w.r.t. {@link Remap}). 
 * In many cases, a {@linkplain Reference2ReferenceMaps#singleton(Object, Object) singleton map} 
 * will be appropriate.
 * 
 * @author Sebastiano Vigna
 * @since 2.2
 */
public class RemappingDocumentIterator implements DocumentIterator {

	/** The underlying document iterator. */
	private final DocumentIterator documentIterator;

	/** If not <code>null</code>, the sole external index involved in this iterator. */
	final private Index soleIndex;

	/** The set of external indices. */
	final private ReferenceSet<Index> indices;

	/** A map from external to internal indices. */
	final private Reference2ReferenceMap<? extends Index, ? extends Index> indexInverseRemapping;

	/** A map from external indices to the iterators already returned for the current document. The key set may
	 * not contain an index because the related iterator has never been requested. */
	final private Index2IntervalIteratorMap currentIterators;
	
	/** An unmodifiable wrapper around {@link #currentIterators}. */
	final private Reference2ReferenceMap<Index,IntervalIterator> unmodifiableCurrentIterators;

	/** Creates a new remapping document iterator wrapping a given document iterator and remapping interval-iterator requests
	 * through a given mapping from external to internal indices.
	 * 
	 * @param documentIterator the underlying document iterator.
	 * @param indexInverseRemapping the mapping from external to internal indices.
	 */
	public RemappingDocumentIterator( final DocumentIterator documentIterator, final Reference2ReferenceMap<? extends Index, ? extends Index> indexInverseRemapping ) {
		this.documentIterator = documentIterator;
		this.indexInverseRemapping = indexInverseRemapping;
		final int n = documentIterator.indices().size();
		this.currentIterators = new Index2IntervalIteratorMap( n );
		this.unmodifiableCurrentIterators = Reference2ReferenceMaps.unmodifiable( currentIterators );

		indices = new ReferenceArraySet<Index>( documentIterator.indices().size() );
		final ReferenceArraySet<Index> nonIndices = new ReferenceArraySet<Index>();
		for( Map.Entry<? extends Index, ? extends Index> e : indexInverseRemapping.entrySet() ) {
			if ( documentIterator.indices().contains( e.getKey() ) ) throw new IllegalArgumentException( "You cannot remap index " + e.getValue() + " to index " + e.getKey() + " as the latter already belongs to the document iterator" );
			if ( ! documentIterator.indices().contains( e.getValue() ) ) throw new IllegalArgumentException( "You cannot remap index " + e.getValue() + " to index " + e.getKey() + " as the former does not belong to the document iterator" );
			nonIndices.add( e.getValue() );
			indices.add( e.getKey() );
		}
		
		for( Index index: documentIterator.indices() ) if ( ! nonIndices.contains( index ) ) indices.add( index );
		soleIndex = n == 1 ? indices.iterator().next() : null;
	}
	
	public long document() {
		return documentIterator.document();
	}

	private Index remapIndex( final Index index ) {
		final Index result = indexInverseRemapping.get(  index  );
		return result == null ? index : result;
	}
	
	public ReferenceSet<Index> indices() {
		return indices;
	}
	
	public IntervalIterator intervalIterator( final Index index ) throws IOException {
		if ( ! indices.contains( index ) ) return IntervalIterators.FALSE;
		final Index remappedIndex = remapIndex( index );

		IntervalIterator intervalIterator = currentIterators.get( index );
		if ( intervalIterator == null ) currentIterators.put( index, intervalIterator = documentIterator.intervalIterator( remappedIndex ) );
		return intervalIterator;
	}
		
	public IntervalIterator intervalIterator() throws IOException {
		if ( soleIndex == null ) throw new IllegalStateException();
		return intervalIterator( soleIndex );
	}

	public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
		for( Index i : indices ) intervalIterator( i );
		return unmodifiableCurrentIterators;
	}
	
	public long nextDocument() throws IOException {
		currentIterators.clear();
		return documentIterator.nextDocument();
	}
	
	public boolean mayHaveNext() {
		return documentIterator.mayHaveNext();
	}
	
	@Override
	public long skipTo( final long n ) throws IOException {
		if ( documentIterator.document() >= n ) return documentIterator.document();
		currentIterators.clear();
		return documentIterator.skipTo( n );
	}
	
	public void dispose() throws IOException {
		documentIterator.dispose();
	}

	public <T> T accept( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return documentIterator.accept( visitor );
	}

	public <T> T acceptOnTruePaths( DocumentIteratorVisitor<T> visitor ) throws IOException {
		return documentIterator.acceptOnTruePaths( visitor );
	}

	public IntervalIterator iterator() {
		try {
			return intervalIterator();
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}

	public double weight() {
		return documentIterator.weight();
	}

	public DocumentIterator weight( final double weight ) {
		return documentIterator.weight( weight );
	}

	public String toString() {
		return this.getClass().getSimpleName() + "(" + documentIterator + ", " + indexInverseRemapping + ")";
	}
}
