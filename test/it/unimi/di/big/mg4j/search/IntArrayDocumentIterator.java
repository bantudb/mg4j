package it.unimi.di.big.mg4j.search;

import static it.unimi.di.big.mg4j.search.IntArrayIndexIterator.INDEX;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.fastutil.objects.ReferenceSets;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.util.Intervals;

import java.util.Arrays;

/** A partially implemented {@link DocumentIterator document iterator} that returns
 * a given list of documents and associated intervals.
 * 
 */

public class IntArrayDocumentIterator implements DocumentIterator {

	private final long[] document;
	private final int[][][] interval;
	private final int[][][] intervalTerm;

	private int curr = -1;
	private IntervalIterator currentIterator;
	private double weight;	

	public double weight() {
		return weight;
	}
	
	public IntArrayDocumentIterator weight( final double weight ) {
		this.weight = weight;
		return this;
	}

	/** Creates a new array-based document iterator without interval terms.
	 * 
	 * @param document an (increasing) array of documents that will be returned.
	 * @param interval a parallel array of arrays of intervals, represented by one-element (singleton) or two-element arrays.
	 */
	
	public IntArrayDocumentIterator( final long[] document, final int[][][] interval ) {
		this( document, interval, null );
	}
	
	/** Creates a new array-based document iterator.
	 * 
	 * @param document an (increasing) array of documents that will be returned.
	 * @param interval a parallel array of arrays of intervals, represented by one-element (singleton) or two-element arrays.
	 * @param intervalTerm a parallel array of arrays of interval terms, represented as arrays.
	 */
	
	public IntArrayDocumentIterator( final long[] document, final int[][][] interval, final int[][][] intervalTerm ) {
		this.document = document;
		this.interval = interval;
		this.intervalTerm = intervalTerm;
		if( document.length != interval.length ) throw new IllegalArgumentException();
		if( intervalTerm != null && document.length != intervalTerm.length ) throw new IllegalArgumentException();
		for( int i = 0; i < document.length - 1; i++ ) if ( document[ i ] >= document[ i + 1 ] ) throw new IllegalArgumentException( "Document array is not increasing" );
		// Check for antichains
		for( int i = 0; i < document.length; i++ ) {
			// If there is an empty interval, it must be the only one
			for ( int j = 0; j < interval[ i ].length; j++ ) { 
				if ( interval[ i ][ j ].length == 0 && interval[ i ].length != 1 ) throw new IllegalArgumentException( "Empty interval in a non-singleton antichain" );
				if ( j > 0 && ( interval[ i ][ j ][ 0 ] <= interval[ i ][ j-1 ][ 0 ] || interval[ i ][ j ][ interval[ i ][ j ].length - 1 ] <= interval[ i ][ j-1 ][ interval[ i ][ j-1 ].length - 1 ] ) ) throw new IllegalArgumentException( "Not an antichain: " + Arrays.toString( interval[ i ][ j - 1 ] ) + " vs. " + Arrays.toString( interval[ i ][ j ] ) );
			}
			
			if ( intervalTerm != null && interval[ i ].length != intervalTerm[ i ].length ) throw new IllegalArgumentException();
		}
	}
	
	@Override
	public boolean mayHaveNext() {
		return curr < document.length - 1; 
	}

	@Override
	public long nextDocument() {
		if ( ! mayHaveNext()) return END_OF_LIST;
		curr++;
		currentIterator = null;
		return document[ curr ];
	}

	@Override
	public long skipTo( long n ) {
		if ( curr != -1 && document[ curr ] >= n ) return document[ curr ];
		long result;
		while ( mayHaveNext() ) if ( ( result = nextDocument() ) >= n ) return result;
		return DocumentIterator.END_OF_LIST;
	}

	public <T> T accept(DocumentIteratorVisitor<T> visitor) {
		if ( ! visitor.visitPre( this ) ) return null;
		return visitor.visitPost( this, null );
	}

	public <T> T acceptOnTruePaths(DocumentIteratorVisitor<T> visitor) {
		if ( ! visitor.visitPre( this ) ) return null;
		return visitor.visitPost( this, null );
	}

	public void dispose() {}

	public long document() {
		if ( curr == -1 ) return -1;
		if ( curr == END_OF_LIST ) return END_OF_LIST;
		return document[ curr ];
	}
	
	public ReferenceSet<Index> indices() {
		return ReferenceSets.singleton( INDEX );
	}

	public static class ArrayIntervalIterator implements IntervalIterator {
		private int curr = -1;
		private final int[][] interval;
		private final int[][] intervalTerm;
		private final int extent;

		private static String toString( int[] interval ) {
			if ( interval.length == 0 ) return Intervals.EMPTY_INTERVAL.toString();
			if ( interval.length == 1 ) return Interval.valueOf( interval[ 0 ] ).toString();
			return Interval.valueOf( interval[ 0 ], interval[ 1 ] ).toString();
		}
		
		public ArrayIntervalIterator( int[][] interval, int[][] intervalTerm ) {
			this.interval = interval;
			this.intervalTerm = intervalTerm;
			int e = Integer.MAX_VALUE;
			for( int[] i: interval ) e = Math.min( e, i.length == 1 ? 1 : i[ 1 ] - i[ 0 ] + 1 );
			extent = e;
		}
		
		public int extent() {
			return extent;
		}

		public IntervalIterator reset() {
			curr = -1;
			return this;
		}
		
		public void intervalTerms( final LongSet terms ) {
			if ( intervalTerm == null ) new UnsupportedOperationException();
			for( int e: intervalTerm[ curr ] ) terms.add( e );
		}

		public Interval nextInterval() {
			if ( curr == interval.length - 1 ) return null;
			curr++;
			return interval[ curr ].length == 1 ? Interval.valueOf( interval[ curr ][ 0 ] ) : Interval.valueOf( interval[ curr ][ 0 ], interval[ curr ][ 1 ] );
		}
		
		public String toString() {
			MutableString result = new MutableString();
			result.append( '{' );
			boolean first = true;
			for( int[] i : interval ) {
				if ( ! first ) result.append( ',' );
				first = false;
				result.append( toString( i ) );
			}
			return result.append( '}' ).toString();
		}
	}
	
	public IntervalIterator intervalIterator() {
		if ( curr == -1 ) throw new IllegalStateException();
		if ( currentIterator != null ) return currentIterator;
		if ( interval[ curr ].length == 0 ) return IntervalIterators.FALSE;
		if ( interval[ curr ].length == 1 && interval[ curr ][ 0 ].length == 0 ) return IntervalIterators.TRUE;
		return currentIterator = new ArrayIntervalIterator( interval[ curr ], intervalTerm == null ? null : intervalTerm[ curr ] );
	}

	public IntervalIterator intervalIterator(Index index) {
		return intervalIterator();
	}

	public Reference2ReferenceMap<Index, IntervalIterator> intervalIterators() {
		return Reference2ReferenceMaps.singleton( INDEX, intervalIterator() );	}

	public IntervalIterator iterator() {
		return intervalIterator();
	}
	
	public void reset() {
		curr = -1;
	}
	
	public String toString() {
		MutableString result = new MutableString();
		result.append( '[' );
		for( int i = 0; i < document.length; i++ ) {
			if ( i != 0 ) result.append( ", " );
			result.append( '<' ).append( document[ i ] ).append( ':' ).append( new ArrayIntervalIterator( interval[ i ], intervalTerm[ i ] ) ).append( '>' );
		}
		return result.append( ']' ).toString();
	}

}
