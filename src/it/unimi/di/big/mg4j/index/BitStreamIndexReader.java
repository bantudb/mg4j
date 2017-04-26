
package it.unimi.di.big.mg4j.index;





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


import it.unimi.dsi.bits.Fast;


import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A bitstream-based {@linkplain IndexReader index reader}. */


public class BitStreamIndexReader extends AbstractIndexReader {
 @SuppressWarnings("unused")
 private static final Logger LOGGER = LoggerFactory.getLogger( BitStreamIndexReader.class );

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
 public BitStreamIndexReader( final BitStreamIndex index, final InputBitStream ibs ) {
  this.index = index;
  this.indexIterator = new BitStreamIndexReaderIndexIterator( this, ibs );
 }

 protected static final class BitStreamIndexReaderIndexIterator extends AbstractIndexIterator implements IndexIterator {
  /** The enclosing instance. */
  private final BitStreamIndexReader parent;
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

  /** The cached copy of {@link #index index.hasPositions}. */
  protected final boolean hasPositions;
  /** The cached copy of {@link #index index.hasCounts}. */
  protected final boolean hasCounts;
  /** The cached copy of {@link #index index.hasPayloads}. */
  protected final boolean hasPayloads;
  /** Whether the underlying index has skips. */
  protected final boolean hasSkips;

  /** The cached copy of {@link #index index.pointerCoding}. */
  protected final Coding pointerCoding;

  /** The cached copy of {@link #index index.countCoding}. */
  protected final Coding countCoding;


  /** The cached copy of {@link #index index.positionCoding}. */
  protected final Coding positionCoding;


  /** The payload, in case the index of this reader has payloads, or <code>null</code>. */
  protected final Payload payload;


  /** The parameter <code>b</code> for Golomb coding of pointers. */
  protected int b;
  /** The parameter <code>log2b</code> for Golomb coding of pointers; it is the most significant bit of {@link #b}. */
  protected int log2b;

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


  /** Whether the index will use variable quanta. */
  private boolean variableQuanta;
    /** The parameter <code>h</code> (the maximum height of a skip tower). */
  public final int height;
  /** The quantum. */
  public long quantum;
  /** The bit mask giving the remainder of the division by {@link #quantum}. */
  public long quantumModuloMask;
  /** The shift giving result of the division by {@link #quantum}. */
  public int quantumDivisionShift;
  /** The maximum height of a skip tower in the current block. May be less than {@link #height} if the block is defective,
		 * and will be -1 on defective quanta (no tower at all). */
  private int maxh;
  /** The maximum valid index of the current skip tower, if any. */
  private int s;
  /** The minimum valid index of the current skip tower, or {@link Integer#MAX_VALUE}. If {@link #maxh} is negative, the value is undefined. */
  private int lowest;
  /** We have <var>w</var> = <var>Hq</var>. */
  private long w;
  /** The bit mask giving the remainder of the division by {@link #w}. */
  private long wModuloMask;
  /** The shift giving result of the division by {@link #w}. */
  private int wDivisionShift;
  /** The Golomb modulus for a top pointer skip, for each level. */
  private int[] towerTopB;
  /** The most significant bit of the Golomb modulus for a top point[]er skip, for each level. */
  private int[] towerTopLog2B;
  /** The Golomb modulus for a lower pointer skip, for each level. */
  private int[] towerLowerB;
  /** The most significant bit of the Golomb modulus for a lower pointer skip, for each level. */
  private int[] towerLowerLog2B;
  /** The prediction for a pointer skip, for each level. */
  private long[] pointerPrediction;
  /** An array to decode bit skips. */
  private long[] bitSkip;
  /** An array to decode the pointer skips. */
  private long[] pointerSkip;
  /** The number of bits read just after reading the last skip tower. */
  private long readBitsAtLastSkipTower;
  /** The document pointer corresponding to the last skip tower. */
  private long pointerAtLastSkipTower;
  /** The current quantum bit length, as provided by the index. */
  private int quantumBitLength;
  /** The current entry bit length, as provided by the index. */
  private int entryBitLength;
  /** This value of {@link #state} means that we are positioned just before a tower. */
  private static final int BEFORE_TOWER = 0;

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

  public BitStreamIndexReaderIndexIterator( final BitStreamIndexReader parent, final InputBitStream ibs ) {
   this.parent = parent;
   this.ibs = ibs;
   index = parent.index;
   keyIndex = index.keyIndex;
   pointerCoding = index.pointerCoding;


   hasPayloads = index.hasPayloads;
   payload = hasPayloads ? index.payload.copy() : null;
   hasCounts = index.hasCounts;
   countCoding = index.countCoding;
   hasPositions = index.hasPositions;
   positionCoding = index.positionCoding;
   if ( hasPositions ) positionCache = new int[ POSITION_CACHE_INITIAL_SIZE ];







   intervalIterator = index.hasPositions ? new it.unimi.di.big.mg4j.index.IndexIntervalIterator( this ) : null;
   singletonIntervalIterator = index.hasPositions ? Reference2ReferenceMaps.singleton( keyIndex, (IntervalIterator)intervalIterator ) : null;


   if ( ( index.quantum == -1 ) != ( index.height == -1 ) ) throw new IllegalArgumentException();
   height = index.height;


   hasSkips = quantum != -1 && height != -1;
   if ( hasSkips ) {


    if ( ! ( variableQuanta = index.quantum == 0 ) ) {
     quantum = index.quantum;
     quantumModuloMask = quantum - 1;
     quantumDivisionShift = Fast.mostSignificantBit( quantum );
     w = ( 1L << height ) * quantum;
     wModuloMask = w - 1;
     wDivisionShift = Fast.mostSignificantBit( w );
    }

    bitSkip = new long[ height + 1 ];
    pointerSkip = new long[ height + 1 ];
    towerTopB = new int[ height + 1 ];
    towerTopLog2B = new int[ height + 1 ];
    towerLowerB = new int[ height + 1 ];
    towerLowerLog2B = new int[ height + 1 ];
    pointerPrediction = new long[ height + 1 ];


   }
   else {
    w = wModuloMask = quantumModuloMask = quantumDivisionShift = wDivisionShift = 0;
    bitSkip = null;
    towerTopB = towerTopLog2B = towerLowerB = towerLowerLog2B = null;
    pointerSkip = pointerPrediction = null;
   }

  }


  private void ensureHasPositions() {
   if ( ! hasPositions ) throw new UnsupportedOperationException( "Index " + index + " does not contain positions" );
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

   switch( index.frequencyCoding ) {
   case GAMMA:


    frequency = ibs.readLongGamma() + 1;


    break;
   case SHIFTED_GAMMA:


    frequency = ibs.readLongShiftedGamma() + 1;


    break;
   case DELTA:


    frequency = ibs.readLongDelta() + 1;


    break;
   default:
    throw new IllegalStateException( "The required frequency coding (" + index.frequencyCoding + ") is not supported." );
   }


   hasPointers = frequency < index.numberOfDocuments;

   // We compute the modulus used for pointer Golomb coding 
   if ( pointerCoding == Coding.GOLOMB ) {


    if ( hasPointers ) {
     b = BitStreamIndex.golombModulus( frequency, index.numberOfDocuments );
     log2b = Fast.mostSignificantBit( b );
    }


   }



   if ( hasSkips ) {


    if ( variableQuanta ) {
     quantumDivisionShift = frequency > 1 ? ibs.readGamma() - 1 : -1;
     if ( quantumDivisionShift == -1 ) quantumDivisionShift = Fast.ceilLog2( frequency ) + 1;
     quantum = 1L << quantumDivisionShift;
     quantumModuloMask = quantum - 1;
     w = ( 1L << height ) * quantum;
     wModuloMask = w - 1;
     wDivisionShift = Fast.mostSignificantBit( w );
    }

    quantumBitLength = entryBitLength = -1;
    lowest = Integer.MAX_VALUE;

    if ( ASSERTS ) for( int i = height; i > Math.min( height, Fast.mostSignificantBit( frequency >> quantumDivisionShift ) ); i-- ) pointerPrediction[ i ] = towerTopB[ i ] = towerLowerB[ i ] = -1;

    final long pointerQuantumSigma = BitStreamIndex.quantumSigma( frequency, index.numberOfDocuments, quantum );
    for( int i = Math.min( height, Fast.mostSignificantBit( frequency >> quantumDivisionShift ) ); i >= 0; i-- ) {
     towerTopB[ i ] = BitStreamIndex.gaussianGolombModulus( pointerQuantumSigma, i + 1 );
     towerTopLog2B[ i ] = Fast.mostSignificantBit( towerTopB[ i ] );
     towerLowerB[ i ] = BitStreamIndex.gaussianGolombModulus( pointerQuantumSigma, i );
     towerLowerLog2B[ i ] = Fast.mostSignificantBit( towerLowerB[ i ] );
     pointerPrediction[ i ] = ( quantum * ( 1L << i ) * index.numberOfDocuments + frequency / 2 ) / frequency;
    }


   }




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

   if ( ! hasPayloads )


    throw new UnsupportedOperationException( "This index ("+ index + ") does not contain payloads" );



   if ( state <= BEFORE_PAYLOAD ) {

    if ( state == BEFORE_TOWER ) readTower();


    payload.read( ibs );


    state = hasCounts ? BEFORE_COUNT : BEFORE_POINTER;





   }
   return payload;

  }

  public int count() throws IOException {
   if ( DEBUG ) System.err.println( this + ".count()" );
   if ( count != -1 ) return count;
   if ( ASSERTS ) ensureCurrentDocument();

   if ( ! hasCounts )


    throw new UnsupportedOperationException( "This index (" + index + ") does not contain counts" );




   if ( state == BEFORE_TOWER ) readTower();


   if ( state == BEFORE_PAYLOAD ) payload.read( ibs );


  {

   if ( ASSERTS ) if ( state != BEFORE_COUNT ) throw new IllegalStateException();
   state = hasPositions ? BEFORE_POSITIONS : BEFORE_POINTER;







   switch( countCoding ) {
   case UNARY:


    count = ibs.readUnary() + 1;


    break;
   case SHIFTED_GAMMA:


    count = ibs.readShiftedGamma() + 1;


    break;
   case GAMMA:


    count = ibs.readGamma() + 1;


    break;
   case DELTA:


    count = ibs.readDelta() + 1;


    break;
   default: throw new IllegalStateException( "The required count coding (" + countCoding + ") is not supported." );
   }

  }

   return count;

  }



  /** We read positions, assuming state <= BEFORE_POSITIONS */
  protected void updatePositionCache() throws IOException {
   if ( ASSERTS ) assert state <= BEFORE_POSITIONS;

   if ( ! hasPositions )


    throw new UnsupportedOperationException( "Index " + index + " does not contain positions" );


   currentPosition = 0;
   if ( state < BEFORE_POSITIONS ) {

    if ( state == BEFORE_TOWER ) readTower();


    if ( state == BEFORE_PAYLOAD ) payload.read( ibs );

    if ( state == BEFORE_COUNT )
  {

   if ( ASSERTS ) if ( state != BEFORE_COUNT ) throw new IllegalStateException();







   switch( countCoding ) {
   case UNARY:


    count = ibs.readUnary() + 1;


    break;
   case SHIFTED_GAMMA:


    count = ibs.readShiftedGamma() + 1;


    break;
   case GAMMA:


    count = ibs.readGamma() + 1;


    break;
   case DELTA:


    count = ibs.readDelta() + 1;


    break;
   default: throw new IllegalStateException( "The required count coding (" + countCoding + ") is not supported." );
   }

  }

   }
    if ( count > positionCache.length ) positionCache = new int[ Math.max( positionCache.length * 2, count ) ];
    final int[] occ = positionCache;
    state = BEFORE_POINTER;


    switch( positionCoding ) {
    case SHIFTED_GAMMA:


     ibs.readShiftedGammas( occ, count );
     for( int i = 1; i < count; i++ ) occ[ i ] += occ[ i - 1 ] + 1;


     return;
    case GAMMA:


     ibs.readGammas( occ, count );
     for( int i = 1; i < count; i++ ) occ[ i ] += occ[ i - 1 ] + 1;


     return;
    case DELTA:


     ibs.readDeltas( occ, count );
     for( int i = 1; i < count; i++ ) occ[ i ] += occ[ i - 1 ] + 1;


     return;
    case GOLOMB:


     if ( ASSERTS ) assert index.sizes != null;
     int docSize = index.sizes.getInt( currentDocument );
     if ( count < 3 ) for( int i = 0; i < count; i++ ) occ[ i ] = ibs.readMinimalBinary( docSize );
     else {
      final int bb = BitStreamIndex.golombModulus( count, docSize );
      int prev = -1;
      if ( bb != 0 ) {
       final int log2bb = Fast.mostSignificantBit( bb );
       for( int i = 0; i < count; i++ ) occ[ i ] = prev = ibs.readGolomb( bb, log2bb ) + prev + 1;
      }
      else for ( int i = 0; i < count; i++ ) occ[ i ] = i;
     }


     return;
    case SKEWED_GOLOMB:


     if ( ASSERTS ) assert index.sizes != null;
     int docSize2 = index.sizes.getInt( currentDocument );
     if ( count < 3 ) for( int i = 0; i < count; i++ ) occ[ i ] = ibs.readMinimalBinary( docSize2 );
     else {
      final int sb = ibs.readMinimalBinary( docSize2 ) + 1;

      int prev2 = -1;
      for( int i = 0; i < count; i++ ) occ[ i ] = prev2 = ibs.readSkewedGolomb( sb ) + prev2 + 1;
     }


     return;
    case INTERPOLATIVE:


     it.unimi.di.big.mg4j.io.InterpolativeCoding.read( ibs, occ, 0, count, 0, index.sizes.getInt( currentDocument ) - 1 );


     return;
    default:
     throw new IllegalStateException( "The required position coding (" + index.positionCoding + ") is not supported." );
    }


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


    if ( state == BEFORE_TOWER ) readTower();


    if ( state == BEFORE_PAYLOAD ) {
     payload.read( ibs );
     state = hasCounts ? BEFORE_COUNT : BEFORE_POINTER;
    }







    if ( state == BEFORE_COUNT )
  {

   if ( ASSERTS ) if ( state != BEFORE_COUNT ) throw new IllegalStateException();
   state = hasPositions ? BEFORE_POSITIONS : BEFORE_POINTER;







   switch( countCoding ) {
   case UNARY:


    count = ibs.readUnary() + 1;


    break;
   case SHIFTED_GAMMA:


    count = ibs.readShiftedGamma() + 1;


    break;
   case GAMMA:


    count = ibs.readGamma() + 1;


    break;
   case DELTA:


    count = ibs.readDelta() + 1;


    break;
   default: throw new IllegalStateException( "The required count coding (" + countCoding + ") is not supported." );
   }

  }




    if ( state == BEFORE_POSITIONS ) {
     state = BEFORE_POINTER;


     switch( positionCoding ) {
     case SHIFTED_GAMMA:


      ibs.skipShiftedGammas( count );


      break;
     case GAMMA:


      ibs.skipGammas( count );


      break;
     case DELTA:


      ibs.skipDeltas( count );


    break;
     case GOLOMB:


      if ( ASSERTS ) assert index.sizes != null;
      int docSize = index.sizes.getInt( currentDocument );
      if ( count < 3 ) for( int i = 0; i < count; i++ ) ibs.readMinimalBinary( docSize );
      else {
       final int bb = BitStreamIndex.golombModulus( count, docSize );

       if ( bb != 0 ) {
        final int log2bb = Fast.mostSignificantBit( bb );
        for( int i = 0; i < count; i++ ) ibs.readGolomb( bb, log2bb );
       }
      }


      break;
     case SKEWED_GOLOMB:


      if ( ASSERTS ) assert index.sizes != null;
      docSize = index.sizes.getInt( currentDocument );
      if ( count < 3 ) for( int i = 0; i < count; i++ ) ibs.readMinimalBinary( docSize );
      else {
       final int sb = ibs.readMinimalBinary( docSize ) + 1;
       for( int i = 0; i < count; i++ ) ibs.readSkewedGolomb( sb );
      }


      break;
     case INTERPOLATIVE:


      it.unimi.di.big.mg4j.io.InterpolativeCoding.read( ibs, null, 0, count, 0, index.sizes.getInt( currentDocument ) - 1 );


      break;
     default:
      throw new IllegalStateException( "The required position coding (" + positionCoding + ") is not supported." );
     }

    }


   }


   if ( currentDocument == END_OF_LIST ) return END_OF_LIST;
   if ( ++numberOfDocumentRecord == frequency ) return currentDocument = END_OF_LIST;

   if ( hasPointers ) {// We do not write pointers for everywhere occurring terms.

    switch( pointerCoding ) {
    case UNARY:


     currentDocument += ibs.readLongUnary() + 1;


     break;
    case SHIFTED_GAMMA:


     currentDocument += ibs.readLongShiftedGamma() + 1;


     break;
    case GAMMA:


     currentDocument += ibs.readLongGamma() + 1;


     break;
    case DELTA:


     currentDocument += ibs.readLongDelta() + 1;


     break;
    case GOLOMB:


     currentDocument += ibs.readLongGolomb( b, log2b ) + 1;


     break;
    default:
     throw new IllegalStateException( "The required pointer coding (" + pointerCoding + ") is not supported." );
    }

   }
   else currentDocument++;


   if ( hasPayloads )


    state = BEFORE_PAYLOAD;


   else if ( hasCounts )


    state = BEFORE_COUNT;
   count = -1;



   if ( hasSkips && ( numberOfDocumentRecord & quantumModuloMask ) == 0 ) state = BEFORE_TOWER;




   return currentDocument;
  }


  /** Reads the entire skip tower for the current position. 
		 */
  private void readTower() throws IOException {
   readTower( -1 );
  }

  /** Reads the skip tower for the current position, possibly skipping part of the tower. 
		 * 
		 * <P>Note that this method will update {@link #state} only if it reads the entire tower,
		 * otherwise the state remains {@link #BEFORE_TOWER}.
		 * 
		 * @param pointer the tower will be read up to the first entry smaller than or equal to this pointer; use
		 * -1 to guarantee that the entire tower will be read.
		 */

  private void readTower( final long pointer ) throws IOException {
   int i, j, towerLength = 0;
   long cacheOffset, cache, k, bitsAtTowerStart = 0;
   boolean truncated = false;

   if ( ASSERTS ) assert numberOfDocumentRecord % quantum == 0;
   if ( ASSERTS ) if ( state != BEFORE_TOWER ) throw new IllegalStateException( "readTower() called in state " + state );

   cacheOffset = numberOfDocumentRecord & wModuloMask;
   k = cacheOffset >> quantumDivisionShift;
   if ( ASSERTS ) if ( k == 0 ) { // Invalidate current tower data
    java.util.Arrays.fill( pointerSkip, Long.MAX_VALUE );
    java.util.Arrays.fill( bitSkip, Long.MAX_VALUE );
   }

   // Compute the height of the current skip tower.
   s = ( k == 0 )? height : Long.numberOfTrailingZeros( k );

   cache = frequency - w * ( numberOfDocumentRecord >> wDivisionShift );
   if ( cache < w ) {
    maxh = Fast.mostSignificantBit( ( cache >> quantumDivisionShift ) - k );
    if ( maxh < s ) {
     s = maxh;
     truncated = true;
    } else truncated = false;
   }
   else {
    cache = w;
    maxh = height;
    truncated = k == 0;
   }

   //assert w == cache || k == 0 || lastMaxh == Fast.mostSignificantBit( k ^ ( cache/quantum ) )  : lastMaxh +","+ (Fast.mostSignificantBit( k ^ ( cache/quantum ) ));

   i = s;

   if ( s >= 0 ) {
    if ( k == 0 ) {
     if ( quantumBitLength < 0 ) {
      quantumBitLength = ibs.readDelta();
      entryBitLength = ibs.readDelta();
     }
     else {
      quantumBitLength += Fast.nat2int( ibs.readDelta() );
      entryBitLength += Fast.nat2int( ibs.readDelta() );
     }
     if ( DEBUG ) System.err.println( "{" + this + "} quantum bit length=" + quantumBitLength + " entry bit length=" + entryBitLength );
    }

    if ( DEBUG ) System.err.println( "{" + this + "} Reading tower; pointer=" + pointer + " maxh=" + maxh + " s=" + s );

    if ( s > 0 ) {
     towerLength = entryBitLength * ( s + 1 ) + Fast.nat2int( ibs.readDelta() );
     if ( DEBUG ) System.err.println( "{" + this + "} Tower length=" + towerLength );
    }

    // We store the number of bits read at the start of the tower (just after the length).
    bitsAtTowerStart = ibs.readBits();

    if ( truncated ) {
     if ( DEBUG ) System.err.println( "{" + this + "} Truncated--reading tops" );
     // We read the tower top.
     pointerSkip[ s ] = Fast.nat2int( ibs.readGolomb( towerTopB[ s ], towerTopLog2B[ s ] ) ) + pointerPrediction[ s ];
     bitSkip[ s ] = quantumBitLength * ( 1L << s ) + entryBitLength * ( ( 1L << s + 1 ) - s - 2 ) + Fast.nat2int( ibs.readLongDelta() );
    }
    else {
     // We copy the tower top from the lowest inherited entry suitably updated.
     pointerSkip[ s ] = pointerSkip[ s + 1 ] - ( currentDocument - pointerAtLastSkipTower );
     bitSkip[ s ] = bitSkip[ s + 1 ] - ( bitsAtTowerStart - readBitsAtLastSkipTower ) - towerLength;
    }

    // We read the remaining part of the tower, at least until we point after pointer.
    if ( currentDocument + pointerSkip[ i ] > pointer ) {
     for( i = s - 1; i >= 0; i-- ) {
      pointerSkip[ i ] = Fast.nat2int( ibs.readGolomb( towerLowerB[ i ], towerLowerLog2B[ i ] ) ) + pointerSkip[ i + 1 ] / 2;
      bitSkip[ i ] = ( bitSkip[ i + 1 ] - entryBitLength * ( i + 1 ) ) / 2 - Fast.nat2int( ibs.readLongDelta() );
      if ( DEBUG ) if ( currentDocument + pointerSkip[ i ] <= pointer ) System.err.println( "{" + this + "} stopping reading at i=" + i + " as currentDocument (" + currentDocument + ") plus pointer skip (" + pointerSkip[ i ] + ") is smaller than or equal target (" + pointer +")" );
      if ( currentDocument + pointerSkip[ i ] <= pointer ) break;
     }
    }
   }

   /* If we did not read the entire tower, we need to fix the skips we read (as they
			 * are offsets from the *end* of the tower) and moreover we must make unusable the
			 * rest of the tower (for asserts). */
   if ( i > 0 ) {
    final long fix = ibs.readBits() - bitsAtTowerStart;
    for( j = s; j >= i; j-- ) bitSkip[ j ] += towerLength - fix;
    if ( ASSERTS ) for( ; j >= 0; j-- ) pointerSkip[ j ] = Long.MAX_VALUE;
   }
   else

    state = hasPayloads ? BEFORE_PAYLOAD : hasCounts ? BEFORE_COUNT : BEFORE_POINTER;
   // We update the inherited tower.
   final long deltaBits = ibs.readBits() - readBitsAtLastSkipTower;
   final long deltaPointers = currentDocument - pointerAtLastSkipTower;

   for( j = Fast.mostSignificantBit( k ^ ( cache >> quantumDivisionShift ) ); j >= s + 1; j-- ) {
    bitSkip[ j ] -= deltaBits;
    pointerSkip[ j ] -= deltaPointers;
   }

   readBitsAtLastSkipTower = ibs.readBits();
   pointerAtLastSkipTower = currentDocument;

   lowest = i < 0 ? 0 : i;

   if ( DEBUG ) {
    System.err.println( "{" + this + "} " + "Computed skip tower (lowest: " + lowest + ") for document record number " + numberOfDocumentRecord + " (pointer " + currentDocument + ") from " + Math.max( i , 0 ) + ": " );
    System.err.print( "% " );
    for( j = 0; j <= s; j++ ) System.err.print( pointerSkip[ j ] + ":" + bitSkip[ j ] + " " );
    System.err.print( " [" );
    for( ; j <= height; j++ ) System.err.print( pointerSkip[ j ] + ":" + bitSkip[ j ] + " " );
    System.err.print( "]" );
    System.err.println();
   }
  }

  /*
		public int skip( final int n ) throws IOException {
		 	long k, cacheOffset; 
			int i, start = numberOfDocumentRecord, skip = 0;

			if ( DEBUG ) System.err.println( "{" + this + "} " + "Going to enter linear skip code with lastDoc=" + currentDocument + ", numberOfDocumentRecord=" + numberOfDocumentRecord + ", n=" + n );
			if ( n < 0 ) throw new IllegalArgumentException();
			if ( n == 0 ) return 0;
			
			// If we are just at the start of a list, let us read the first pointer.
			if ( numberOfDocumentRecord == -1 ) readDocumentPointer();
			if ( state == BEFORE_TOWER ) readTower( -1 ); 

			if ( DEBUG ) System.err.println( "{" + this + "} " + "Entering skip code with lastDoc=" + currentDocument + ", numberOfDocumentRecord=" + numberOfDocumentRecord + ", n=" + n );

			for(;;) {
				if ( DEBUG ) System.err.println( "{" + this + "} " + "In for loop, lastDoc=" + currentDocument + ", maxh=" + maxh + ", numberOfDocumentRecord=" + numberOfDocumentRecord + ", n=" + n );

				cacheOffset = numberOfDocumentRecord & wModuloMask;
				k = cacheOffset >> quantumDivisionShift;

				if ( maxh < 0 ) break; // Defective quantum--no tower.

				for( i = Fast.mostSignificantBit( k ^ ( Math.min( w, frequency - numberOfDocumentRecord + cacheOffset ) >> quantumDivisionShift ) ); i >= 0; i-- )
					if ( ( skip = ( ( k & - ( 1 << i ) ) + ( 1 << i ) ) * quantum - cacheOffset ) <= n ) break;
				
				if ( i >= 0 ) {
					ibs.skip( bitSkip[ i ] - ( ibs.readBits() - readBitsAtLastSkipTower ) );
					state = BEFORE_TOWER;
					currentDocument = pointerSkip[ i ] + pointerAtLastSkipTower;
					numberOfDocumentRecord += skip;
					// If we skipped beyond the end of the list, we invalidate the current document.
					if ( numberOfDocumentRecord == frequency ) currentDocument = -1;
					readTower( -1 );
					count = -1; // We must invalidate count as readDocumentPointer() would do.
					if ( endOfList() ) return numberOfDocumentRecord - start;
				}
				else break;
			}

			if ( DEBUG ) System.err.println( "{" + this + "} " + "Completing sequentially, lastDoc=" + currentDocument + ", numberOfDocumentRecord=" + numberOfDocumentRecord + ", n=" + n );

			while( numberOfDocumentRecord - start < n ) {
				if ( endOfList() ) break;
				readDocumentPointer();
			}
			return numberOfDocumentRecord - start;
		}
	*/




  public long skipTo( final long p ) throws IOException {
   if ( DEBUG ) System.err.println( this + ".skipTo(" + p + ") [currentDocument=" + currentDocument + ", numberOfDocumentRecord=" + numberOfDocumentRecord );

   // If we are just at the start of a list, let us read the first pointer.
   if ( numberOfDocumentRecord == -1 ) nextDocument(); // TODO: shouldn't we just read the tower?

   if ( currentDocument >= p ) {
    if ( DEBUG ) System.err.println( this + ": No skip necessary, returning " + currentDocument );
    return currentDocument;
   }


   if ( hasSkips ) {



    if ( state == BEFORE_TOWER ) readTower( p );

    final long[] pointerSkip = this.pointerSkip;

    for(;;) {
     if ( ASSERTS ) assert maxh < 0 || lowest > 0 || pointerSkip[ 0 ] != Long.MAX_VALUE;

     // If on a defective quantum (no tower) or p is inside the current quantum (no need to scan the tower) we bail out. 
     if ( maxh < 0 || lowest == 0 && pointerAtLastSkipTower + pointerSkip[ 0 ] > p ) break;

     if ( DEBUG ) System.err.println( this + ": In for loop, currentDocument=" + currentDocument + ", maxh=" + maxh + ", numberOfDocumentRecord=" + numberOfDocumentRecord + ", p=" + p );

     final long cacheOffset = numberOfDocumentRecord & wModuloMask;
     final long k = cacheOffset >> quantumDivisionShift;
     final int top = Fast.mostSignificantBit( k ^ ( Math.min( w, frequency - numberOfDocumentRecord + cacheOffset ) >> quantumDivisionShift ) );

     int i;

     for( i = lowest; i <= top; i++ ) {
      if ( ASSERTS ) if ( ( k & 1L << i ) != 0 ) assert pointerSkip[ i ] == pointerSkip[ i + 1 ];
      if ( ASSERTS ) assert pointerSkip[ i ] != Long.MAX_VALUE : "Invalid pointer skip " + i + " (lowest=" + lowest + ", top=" + top + ")";
      if ( pointerAtLastSkipTower + pointerSkip[ i ] > p ) break;
     }

     if ( --i < 0 ) break;

     if ( ASSERTS ) assert i >= lowest : i + " < " + lowest;

     if ( DEBUG ) System.err.println( this + ": Safely after for with i=" + i + ", P[i]=" + pointerSkip[i] + ", A[i]=" + bitSkip[i] );
     if ( DEBUG ) System.err.println( this + ": [" + ibs.readBits() + "] Skipping " + ( bitSkip[ i ] - ( ibs.readBits() - readBitsAtLastSkipTower ) ) + " bits (" + ( ( ( k & - ( 1L << i ) ) + ( 1L << i ) ) * quantum - cacheOffset ) + " records) to get to document pointer " + ( currentDocument + pointerSkip[ i ] ) );

     ibs.skip( bitSkip[ i ] - ( ibs.readBits() - readBitsAtLastSkipTower ) );
     state = BEFORE_TOWER;
     currentDocument = pointerSkip[ i ] + pointerAtLastSkipTower;
     numberOfDocumentRecord += ( ( k & - ( 1L << i ) ) + ( 1L << i ) ) * quantum - cacheOffset;
     // If we skipped beyond the end of the list, we invalidate the current document.
     if ( numberOfDocumentRecord == frequency ) {
      currentDocument = END_OF_LIST;
      state = BEFORE_POINTER; // We are actually before a frequency, but we must avoid that calls to nextDocument() read anything
     }
     else readTower( p ); // Note that if we are exactly on the destination pointer, we will read the entire tower.
     count = -1; // We must invalidate count as readDocumentPointer() would do.
     if ( numberOfDocumentRecord >= frequency - 1 ) break;
    }

    if ( DEBUG ) System.err.println( this + ": Completing sequentially, currentDocument=" + currentDocument + ", numberOfDocumentRecord=" + numberOfDocumentRecord + ", p=" + p );


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

   ensureHasPositions();




   return singletonIntervalIterator;

  }

  public IntervalIterator intervalIterator() throws IOException {
   return intervalIterator( keyIndex );
  }

  public IntervalIterator intervalIterator( final Index index ) throws IOException {
   ensureCurrentDocument();





   if ( index != keyIndex ) return IntervalIterators.FALSE;

   if ( ! hasPositions ) return IntervalIterators.FALSE;

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
