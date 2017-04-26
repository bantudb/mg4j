


package it.unimi.di.big.mg4j.index.wired;



/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2003-2015 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.ReferenceSet;
import it.unimi.di.big.mg4j.index.AbstractIndexIterator;
import it.unimi.di.big.mg4j.index.AbstractIndexReader;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.di.big.mg4j.search.IntervalIterator;
import it.unimi.di.big.mg4j.search.IntervalIterators;





import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





public class GammaDeltaGammaDeltaBitStreamIndexReader extends AbstractIndexReader {
 @SuppressWarnings("unused")
 private static final Logger LOGGER = LoggerFactory.getLogger( GammaDeltaGammaDeltaBitStreamIndexReader.class );

 /** The reference index. */
 protected final BitStreamIndex index;

 private final static boolean ASSERTS = false;
 private final static boolean DEBUG = false;

 /** The {@link IndexIterator} view of this reader (returned by {@link #documents(CharSequence)}). */
 protected final BitStreamIndexReaderIndexIterator indexIterator;

 /** Creates a new skip index reader, with the specified underlying {@link Index} and input bit stream.
	 *
	 * @param index the index.
	 * @param ibs the underlying bit stream.
	 */
 public GammaDeltaGammaDeltaBitStreamIndexReader( final BitStreamIndex index, final InputBitStream ibs ) {
  this.index = index;
  this.indexIterator = new BitStreamIndexReaderIndexIterator( this, ibs );
 }

 protected static final class BitStreamIndexReaderIndexIterator extends AbstractIndexIterator implements IndexIterator {
  /** The enclosing instance. */
  private final GammaDeltaGammaDeltaBitStreamIndexReader parent;
  /** The reference index. */
  protected final BitStreamIndex index;
  /** The underlying input bit stream. */
  protected final InputBitStream ibs;
  /** The enclosed interval iterator. */
  private final it.unimi.di.big.mg4j.index.IndexIntervalIterator intervalIterator;
  /** A singleton set containing the enclosed interval iterator. */
  private final Reference2ReferenceMap<Index,IntervalIterator> singletonIntervalIterator;
  /** The key index. */
  private final Index keyIndex;
  /** The cached copy of {@link #index index.pointerCoding}. */
  protected final Coding pointerCoding;

  /** The cached copy of {@link #index index.countCoding}. */
  protected final Coding countCoding;


  /** The cached copy of {@link #index index.positionCoding}. */
  protected final Coding positionCoding;
  /** The current term. */
  protected long currentTerm = -1;
  /** The current frequency. */
  protected long frequency;
  /** Whether the current terms has pointers at all (this happens when the {@link #frequency} is smaller than the number of documents). */
  protected boolean hasPointers;
  /** The current count (if this index contains counts). */
  protected int count;
  /** The last document pointer we read from current list, -1 if we just read the frequency,
		 * {@link #END_OF_LIST} if we are beyond the end of list. */
  protected long currentDocument;
  /** The number of the document record we are going to read inside the current inverted list. */
  protected long numberOfDocumentRecord;
  /** This variable tracks the current state of the reader. */
  protected int state;
  /** The initial size of {@link #positionCache}. */
  private static final int POSITION_CACHE_INITIAL_SIZE = 16;

  /** The index of the next position to be returned by {@link #nextPosition}. */
  protected int currentPosition;

  /** This value of {@link #state} can be assumed only in indices that contain a payload; it
		 * means that we are positioned just before the payload for the current document record. */
  private static final int BEFORE_PAYLOAD = 1;

  /** This value of {@link #state} can be assumed only in indices that contain counts; it
		 * means that we are positioned just before the count for the current document record. */
  private static final int BEFORE_COUNT = 2;

  /** This value of {@link #state} can be assumed only in indices that contain document positions; 
		 * it means that we are positioned just before the position list of the current document record. */
  private static final int BEFORE_POSITIONS = 3;

  /** This value of {@link #state} means that we are at the start of a new document record, 
		 * unless we already read all documents (i.e., {@link #numberOfDocumentRecord} == {@link #frequency}),
		 * in which case we are at the end of the inverted list, and {@link #currentDocument} is {@link #END_OF_LIST}. */
  private static final int BEFORE_POINTER = 4;

  /** The cached position array. */
  protected int[] positionCache;

  public BitStreamIndexReaderIndexIterator( final GammaDeltaGammaDeltaBitStreamIndexReader parent, final InputBitStream ibs ) {
   this.parent = parent;
   this.ibs = ibs;
   index = parent.index;
   keyIndex = index.keyIndex;
   pointerCoding = index.pointerCoding;
   if ( index.hasPayloads ) throw new IllegalStateException();
   if ( ! index.hasCounts ) throw new IllegalStateException();
   countCoding = index.countCoding;
   if ( ! index.hasPositions ) throw new IllegalStateException();
   positionCoding = index.positionCoding;
   positionCache = new int[ POSITION_CACHE_INITIAL_SIZE ];

   intervalIterator = index.hasPositions ? new it.unimi.di.big.mg4j.index.IndexIntervalIterator( this ) : null;
   singletonIntervalIterator = index.hasPositions ? Reference2ReferenceMaps.singleton( keyIndex, (IntervalIterator)intervalIterator ) : null;
  }







  /** Positions the index on the inverted list of a given term.
		 *
		 * <p>This method can be called at any time. Note that it is <em>always</em> possible
		 * to call this method with argument 0, even if offsets have not been loaded.
		 *
		 * @param term a term.
		 */

  protected void position( final long term ) throws IOException {
   if ( term == 0 ) {
    ibs.position( 0 );
    ibs.readBits( 0 );
   }
   else {
    if ( index.offsets == null ) throw new IllegalStateException( "You cannot position an index without offsets" );
    final long offset = index.offsets.getLong( term );
    ibs.position( offset );
    // TODO: Can't we set this to 0?
    ibs.readBits( offset );
   }

   currentTerm = term;
   readFrequency();
  }

  public long termNumber() {
   return currentTerm;
  }

  protected IndexIterator advance() throws IOException {
   if ( currentTerm == index.numberOfTerms - 1 ) return null;
   if ( currentTerm != -1 ) {
    skipTo( END_OF_LIST );
    nextDocument(); // This guarantees we have no garbage before the frequency
   }
   currentTerm++;
   readFrequency();
   return this;
  }

  private void readFrequency() throws IOException {
   // Read the frequency





    frequency = ibs.readLongGamma() + 1;
   hasPointers = frequency < index.numberOfDocuments;
   count = -1;

   currentDocument = -1;
   numberOfDocumentRecord = -1;
   state = BEFORE_POINTER;
  }

  public Index index() {
   return keyIndex;
  }

  public long frequency() {
   return frequency;
  }

  private void ensureCurrentDocument() {
   if ( ( currentDocument | 0x8000000000000000L ) == -1 ) throw new IllegalStateException( currentDocument == -1 ? "nextDocument() has never been called for term " + currentTerm : "This reader is positioned beyond the end of list of term " + currentTerm );
  }

  public long document() {
   return currentDocument;
  }

  public Payload payload() throws IOException {
   if ( DEBUG ) System.err.println( this + ".payload()" );
   if ( ASSERTS ) ensureCurrentDocument();




    throw new UnsupportedOperationException( "This index ("+ index + ") does not contain payloads" );
  }

  public int count() throws IOException {
   if ( DEBUG ) System.err.println( this + ".count()" );
   if ( count != -1 ) return count;
   if ( ASSERTS ) ensureCurrentDocument();
  {




   state = BEFORE_POSITIONS;
    count = ibs.readGamma() + 1;
  }

   return count;

  }



  /** We read positions, assuming state <= BEFORE_POSITIONS */
  protected void updatePositionCache() throws IOException {
   if ( ASSERTS ) assert state <= BEFORE_POSITIONS;







   currentPosition = 0;
   if ( state < BEFORE_POSITIONS ) {






    if ( state == BEFORE_COUNT )
  {



   state = BEFORE_POSITIONS;
    count = ibs.readGamma() + 1;
  }

   }
    if ( count > positionCache.length ) positionCache = new int[ Math.max( positionCache.length * 2, count ) ];
    final int[] occ = positionCache;
    state = BEFORE_POINTER;
     ibs.readDeltas( occ, count );
     for( int i = 1; i < count; i++ ) occ[ i ] += occ[ i - 1 ] + 1;
  }

  @Override
  public int nextPosition() throws IOException {
   if ( ASSERTS ) ensureCurrentDocument();
   if ( state <= BEFORE_POSITIONS ) updatePositionCache();
   if ( currentPosition == count ) return END_OF_POSITIONS;
   return positionCache[ currentPosition++ ];
  }

  public long nextDocument() throws IOException {
   if ( DEBUG ) System.err.println( "{" + this + "} nextDocument()" );


   if ( state != BEFORE_POINTER ) {
    if ( state == BEFORE_COUNT )
  {




   state = BEFORE_POSITIONS;
    count = ibs.readGamma() + 1;
  }




    if ( state == BEFORE_POSITIONS ) {
     state = BEFORE_POINTER;
      ibs.skipDeltas( count );
    }


   }


   if ( currentDocument == END_OF_LIST ) return END_OF_LIST;
   if ( ++numberOfDocumentRecord == frequency ) return currentDocument = END_OF_LIST;

   if ( hasPointers ) {// We do not write pointers for everywhere occurring terms.
     currentDocument += ibs.readLongDelta() + 1;
   }
   else currentDocument++;
    state = BEFORE_COUNT;
   count = -1;
   return currentDocument;
  }
  public long skipTo( final long p ) throws IOException {
   if ( DEBUG ) System.err.println( this + ".skipTo(" + p + ") [currentDocument=" + currentDocument + ", numberOfDocumentRecord=" + numberOfDocumentRecord );

   // If we are just at the start of a list, let us read the first pointer.
   if ( numberOfDocumentRecord == -1 ) nextDocument(); // TODO: shouldn't we just read the tower?

   if ( currentDocument >= p ) {
    if ( DEBUG ) System.err.println( this + ": No skip necessary, returning " + currentDocument );
    return currentDocument;
   }
   while( currentDocument < p ) nextDocument();

   if ( DEBUG ) System.err.println( this + ".toSkip(): Returning " + currentDocument );
   return currentDocument;
  }

  public void dispose() throws IOException {
   parent.close();
  }

  public boolean mayHaveNext() {
   return numberOfDocumentRecord < frequency - 1;
  }

  public String toString() {
   return index + " [" + currentTerm + "]" + ( weight != 1 ? "{" + weight + "}" : "" );
  }

  public Reference2ReferenceMap<Index,IntervalIterator> intervalIterators() throws IOException {
   intervalIterator();






   return singletonIntervalIterator;

  }

  public IntervalIterator intervalIterator() throws IOException {
   return intervalIterator( keyIndex );
  }

  public IntervalIterator intervalIterator( final Index index ) throws IOException {
   ensureCurrentDocument();





   if ( index != keyIndex ) return IntervalIterators.FALSE;



   if ( ASSERTS ) assert intervalIterator != null;
   intervalIterator.reset();
   return intervalIterator;

  }

  public ReferenceSet<Index> indices() {
   return index.singletonSet;
  }
 }

 private IndexIterator documents( final CharSequence term, final long termNumber ) throws IOException {
  indexIterator.term( term );
  indexIterator.position( termNumber );
  return indexIterator;
 }

 public IndexIterator documents( final long term ) throws IOException {
  return documents( null, term );
 }

 public IndexIterator documents( final CharSequence term ) throws IOException {
  if ( closed ) throw new IllegalStateException( "This " + getClass().getSimpleName() + " has been closed" );
  if ( index.termMap != null ) {
   final long termIndex = index.termMap.getLong( term );
   if ( termIndex == -1 ) return index.getEmptyIndexIterator( term, termIndex );
   return documents( term, termIndex );
  }

  throw new UnsupportedOperationException( "Index " + index + " has no term map" );
 }

  @Override
  public IndexIterator nextIterator() throws IOException {
   return indexIterator.advance();
  }

 public String toString() {
  return getClass().getSimpleName() + "[" + index + "]";
 }

 public void close() throws IOException {
  super.close();
  indexIterator.ibs.close();
 }

}
