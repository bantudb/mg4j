package it.unimi.di.big.mg4j.search;

import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.IndexReader;
import it.unimi.di.big.mg4j.index.NullTermProcessor;
import it.unimi.di.big.mg4j.index.TooManyTermsException;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.dsi.fastutil.objects.ReferenceSets;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Interval;

import java.io.IOException;
import java.util.Arrays;

/** A partially implemented {@link IndexIterator index iterator} that returns
 * a given list of documents and associated positions.
 * 
 */

public class IntArrayIndexIterator implements IndexIterator {
	public final static Index INDEX = new TestIndex();
	
	public static class TestIndex extends Index { 
		private static final long serialVersionUID = 1L;

		public TestIndex() {
			super( Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, null, true, true, NullTermProcessor.getInstance(), "text", null, null, null, null );
		}

		public IndexIterator documents( CharSequence prefix, int limit ) throws IOException, TooManyTermsException {
			throw new UnsupportedOperationException();
		}

		public IndexReader getReader() throws IOException {
			throw new UnsupportedOperationException();
		}

		public IndexReader getReader( int bufferSize ) throws IOException {
			throw new UnsupportedOperationException();
		}
	}	
	
	private final long[] document;
	private final int[][] position;
	
	private int curr = -1;
	private IntervalIterator currentIterator;
	private String term;
	private int id;
	private final int termNumber;
	private double weight;	
	private int pos;
	
	public double weight() {
		return weight;
	}
	
	public IntArrayIndexIterator weight( final double weight ) {
		this.weight = weight;
		return this;
	}
	
	/** Creates a new array-based index iterator with term number 0.
	 * 
	 * @param document an (increasing) array of documents that will be returned.
	 * @param position a parallel array of arrays of positions.
	 */
	
	public IntArrayIndexIterator( long[] document, int[][] position ) {
		this( 0, document, position );
	}
	
	/** Creates a new array-based index iterator.
	 * 
	 * @param termNumber the term number of this iterator.
	 * @param document an (increasing) array of documents that will be returned.
	 * @param position a parallel array of arrays of positions.
	 */
	
	public IntArrayIndexIterator( final int termNumber, long[] document, int[][] position ) {
		this.termNumber = termNumber;
		this.document = document;
		this.position = position;
		if( document.length != position.length ) throw new IllegalArgumentException();
		for( int i = 0; i < document.length - 1; i++ ) if ( document[ i ] >= document[ i + 1 ] ) throw new IllegalArgumentException( "Document array is not increasing" );
		for( int i = 0; i < document.length; i++ )
			for( int j = position[ i ].length - 1; j-- != 0; ) if ( position[ i ][ j ] >= position[ i ][ j +1 ] ) 
				throw new IllegalArgumentException( "Non-increasing position list for document " + i + ": " + Arrays.toString( position[ i ] ) );
	}
	
	public long termNumber() {
		return termNumber;
	}

	@Override
	public boolean mayHaveNext() {
		return curr < document.length - 1; 
	}

	@Override
	public long nextDocument() {
		if ( ! mayHaveNext() ) return END_OF_LIST;
		curr++;
		currentIterator = null;
		pos = 0;
		return document[ curr ];
	}

	@Override
	public long skipTo( long n ) {
		if ( curr != -1 && document[ curr ] >= n ) return document[ curr ];
		long result;
		while ( mayHaveNext() ) if ( ( result = nextDocument() ) >= n ) return result;
		return END_OF_LIST;
	}

	public <T> T accept(DocumentIteratorVisitor<T> visitor) throws IOException {
		return visitor.visit( this );
	}

	public <T> T acceptOnTruePaths(DocumentIteratorVisitor<T> visitor) throws IOException {
		return visitor.visit( this );
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

	public static class ArraySingletonIntervalIterator implements IntervalIterator {
		private int curr = -1;
		private final int[] position;

		public ArraySingletonIntervalIterator( int[] position ) {
			this.position = position;
		}
		
		public int extent() {
			return 1;
		}

		public IntervalIterator reset() {
			curr = -1;
			return this;
		}

		public void intervalTerms( final LongSet terms ) {
			throw new UnsupportedOperationException();
		}

		public Interval nextInterval() {
			if ( curr == position.length - 1 ) return null;
			curr++;
			return Interval.valueOf( position[ curr ] );
		}

		public String toString() {
			return Arrays.toString( position );
		}
	}
	
	public IntervalIterator intervalIterator() {
		if ( curr == -1 ) throw new IllegalStateException();
		if ( currentIterator != null ) return currentIterator;
		if ( position[ curr ].length == 0 ) return IntervalIterators.FALSE;
		return currentIterator = new ArraySingletonIntervalIterator( position[ curr ] );
	}

	public IntervalIterator intervalIterator(Index index) {
		return intervalIterator();
	}

	public Reference2ReferenceMap<Index, IntervalIterator> intervalIterators() {
		throw new UnsupportedOperationException();
	}

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
			result.append( '<' ).append( document[ i ] ).append( ':' ).append( Arrays.toString(  position[ i ] ) ).append( '>' );
		}
		return result.append( ']' ).toString();
	}

	public int count() {
		return position[ curr ].length;
	}

	public long frequency() {
		return document.length;
	}

	public IntArrayIndexIterator id( int id ) {
		this.id = id;
		return this;
	}

	public int id() {
		return id;
	}

	public Index index() {
		return INDEX;
	}

	public Payload payload() {
		return null;
	}

	public String term() {
		return term;
	}

	public IntArrayIndexIterator term( CharSequence term ) {
		this.term = term.toString();
		return this;
	}

	@Override
	public int nextPosition() throws IOException {
		return pos == position[ curr ].length ? END_OF_POSITIONS : position[ curr ][ pos++ ];
	}
}
