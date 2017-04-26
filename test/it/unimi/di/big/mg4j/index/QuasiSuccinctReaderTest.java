package it.unimi.di.big.mg4j.index;

import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndexReader.CountReader;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndexReader.EliasFanoPointerReader;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndexReader.PositionReader;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndexReader.RankedPointerReader;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.util.XorShift128PlusRandom;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;

import org.junit.Test;


public class QuasiSuccinctReaderTest {
	@Test
	public void testNonStrict() throws IOException {
		XorShift128PlusRandom random = new XorShift128PlusRandom( 0 );
		File temp = File.createTempFile( QuasiSuccinctReaderTest.class.getSimpleName(), "test" );
		temp.deleteOnExit();
		for( int length: new int[] { 1, 10, 100, 1000, 10000 } ) {
			for( int log2Quantum: new int[] { 0, 1, 2, 4, 8 } ) {
				for( boolean r: new boolean[] { true, false } ) {
					@SuppressWarnings("resource")
					QuasiSuccinctIndexWriter.Accumulator efa = new QuasiSuccinctIndexWriter.Accumulator( 1024 * 1024, log2Quantum );

					System.err.println( "(nonstrict) size: " + length + " log2Quantum: " + log2Quantum );
					final int[] value = new int[ length ];
					final int cumulative[] = new int[ length + 1 ];
					int s = 0;
					for( int i = 0; i < value.length; i++ )
						cumulative[ i + 1 ] = cumulative[ i ] + ( value[ i ] = ( r ? random.nextInt( 2 ) : random.nextInt( length ) ) + 1 );
					s = cumulative[ length ];
					//System.err.println("upper bound: " + ( s + 1 ) );
					efa.init( length, s + 1, false, true, log2Quantum );
 
					for( int v: value ) efa.add( v );

					@SuppressWarnings("resource")
					FileOutputStream fos = new FileOutputStream( temp );
					QuasiSuccinctIndexWriter.LongWordOutputBitStream lwobs = new QuasiSuccinctIndexWriter.LongWordOutputBitStream( fos.getChannel(), ByteOrder.BIG_ENDIAN );
					int l = QuasiSuccinctIndexWriter.lowerBits( length, s + 1, false );
					final boolean ranked = length + 1L + ( s + 1 >>> l ) + ( length + 1L ) * l > s + 1;
					if ( ranked ) l = 0;

					efa.dump( lwobs );
					lwobs.close();

					//LongArrayBitVector bv = LongArrayBitVector.wrap( bits );
					final long[] bits = BinIO.loadLongs( temp );
					LongBigList list = LongBigArrayBigList.wrap( LongBigArrays.wrap( bits ) );

					final int pointerSize = ranked ? Fast.length( length + 1 ) : QuasiSuccinctIndexWriter.pointerSize( length + 1, s + 1, false, true );
					final long numberOfPointers = ranked ? ( s + 1 >>> log2Quantum ) : QuasiSuccinctIndexWriter.numberOfPointers( length + 1, s + 1, log2Quantum, false, true );

					final QuasiSuccinctIndexReader.LongWordBitReader upperBits = new QuasiSuccinctIndexReader.LongWordBitReader( list, 0 );
					final QuasiSuccinctIndexReader.LongWordBitReader skipPointers = new QuasiSuccinctIndexReader.LongWordBitReader( list, pointerSize );
					final QuasiSuccinctIndexReader.LongWordBitReader lowerBits = new QuasiSuccinctIndexReader.LongWordBitReader( list, l );

					final long skipPointersStart = upperBits.position();
					skipPointers.position( skipPointersStart );
					final long lowerBitsStart = skipPointersStart + pointerSize * numberOfPointers;
					lowerBits.position( lowerBitsStart ); 
					final long upperBitsStart = lowerBitsStart + l * ( length + 1L );
					upperBits.position( upperBitsStart );

					QuasiSuccinctIndexReader.PointerReader pointerReader = ranked ? 
						new QuasiSuccinctIndexReader.RankedPointerReader( list, upperBitsStart, skipPointers, skipPointersStart, numberOfPointers, pointerSize, length, log2Quantum ) :
						new QuasiSuccinctIndexReader.EliasFanoPointerReader( list, lowerBits, lowerBitsStart, l, skipPointers, skipPointersStart, numberOfPointers, pointerSize, length, log2Quantum );

					for( int i = 0; i < length; i++ ) 
						assertEquals( Integer.toString( i + 1 ) + " (" + ranked + ")", cumulative[ i + 1 ], ranked ? ((RankedPointerReader)pointerReader).getNextPrefixSum() : ((EliasFanoPointerReader)pointerReader).getNextPrefixSum() );

					for( int i = 0; i < 10; i++ ) {
						skipPointers.position( skipPointersStart );
						lowerBits.position( lowerBitsStart );
						upperBits.position( upperBitsStart );
						pointerReader = ranked ? 
							new QuasiSuccinctIndexReader.RankedPointerReader( list, upperBitsStart, skipPointers, skipPointersStart, numberOfPointers, pointerSize, length, log2Quantum ) :
							new QuasiSuccinctIndexReader.EliasFanoPointerReader( list, lowerBits, lowerBitsStart, l, skipPointers, skipPointersStart, numberOfPointers, pointerSize, length, log2Quantum );
						long bound = random.nextLong( s + 1 );
						if ( bound == s ) continue;
						//System.err.println( "Bound : " + bound );
						int pos = 1;
						long result;
						while( pos < cumulative.length && cumulative[ pos ] < bound ) pos++;
						if ( pos == cumulative.length ) result = DocumentIterator.END_OF_LIST;
						else result = cumulative[ pos ];
						//System.err.println(Arrays.toString( cumulative ) + " " + bound );
						assertEquals( Integer.toString( i ), result, ranked ? ((RankedPointerReader)pointerReader).skipTo( bound ) : ((EliasFanoPointerReader)pointerReader).skipTo( bound ) );
						if ( result != DocumentIterator.END_OF_LIST ) {
							while( ++pos < cumulative.length )
								assertEquals( Integer.toString( i ), cumulative[ pos ], ranked ? ((RankedPointerReader)pointerReader).getNextPrefixSum() : ((EliasFanoPointerReader)pointerReader).getNextPrefixSum() );
						}
					}
				}
			}
		}
		temp.delete();
	}

	@Test
	public void testQuasiSuccinctCountReader() throws IOException {
		XorShift128PlusRandom random = new XorShift128PlusRandom( 0 );
		File temp = File.createTempFile( QuasiSuccinctReaderTest.class.getSimpleName(), "test" );
		temp.deleteOnExit();

		for( int length: new int[] { 1, 10, 100, 1000, 10000 } ) {
			for( int log2Quantum: new int[] { 0, 1, 2, 4, 8 } ) {
				@SuppressWarnings("resource")
				QuasiSuccinctIndexWriter.Accumulator efa = new QuasiSuccinctIndexWriter.Accumulator( 1024 * 1024, log2Quantum );
				System.err.println( "(strict) size: " + length + " log2Quantum: " + log2Quantum );
				final int[] value = new int[ length ];
				final long cumulative[] = new long[ length + 1 ];
				long s = 0;
				for( int i = 0; i < value.length; i++ ) {
					cumulative[ i + 1 ] = cumulative[ i ] + ( value[ i ] = random.nextInt( length ) + 1 );
				}
				s = cumulative[ length ];
				//System.err.println("upper bound: " + ( s + 1 ) );
				efa.init( length, s, true, false, log2Quantum );
				for( int v: value ) efa.add( v );

				@SuppressWarnings("resource")
				FileOutputStream fos = new FileOutputStream( temp );
				QuasiSuccinctIndexWriter.LongWordOutputBitStream lwobs = new QuasiSuccinctIndexWriter.LongWordOutputBitStream( fos.getChannel(), ByteOrder.BIG_ENDIAN );

				long numBits = efa.dump( lwobs );
				lwobs.close();
				if ( numBits % Long.SIZE != 0 ) numBits += Long.SIZE - numBits % Long.SIZE;

				long[] bits = new long[ (int)( numBits / Long.SIZE ) ];
				assertEquals( bits.length, BinIO.loadLongs( temp, bits ) );
				QuasiSuccinctIndexReader.CountReader quasiSuccinctCountReader = new CountReader( LongBigArrayBigList.wrap( LongBigArrays.wrap( bits ) ), 0, length, (int)s, log2Quantum );
				
				/*LongArrayBitVector bv = LongArrayBitVector.wrap( bits );
				System.err.println( bv );
				System.err.println( Arrays.toString( value ) );
				System.err.println( Arrays.toString( cumulative ) );*/
				for( int i = 0; i < length; i++ ) {
					assertEquals( Integer.toString( i ), value[ i ], quasiSuccinctCountReader.getLong( i ) );
					assertEquals( Integer.toString( i ), cumulative[ i ], quasiSuccinctCountReader.prevPrefixSum + i );
				}
			}
		}
		temp.delete();
	}

	@Test
	public void testQuasiSuccinctPositionReader() throws IOException {
		XorShift128PlusRandom random = new XorShift128PlusRandom( 0 );
		File temp = File.createTempFile( QuasiSuccinctReaderTest.class.getSimpleName(), "test" );
		temp.deleteOnExit();

		for( int length: new int[] { 1, 10, 100, 1000, 10000 } ) {
			for( int log2Quantum: new int[] { 0, 1, 2, 4, 8 } ) {
				@SuppressWarnings("resource")
				QuasiSuccinctIndexWriter.Accumulator efa = new QuasiSuccinctIndexWriter.Accumulator( 1024 * 1024, log2Quantum );
				System.err.println( "(strict) size: " + length + " log2Quantum: " + log2Quantum );
				final int[] value = new int[ length ];
				final long cumulative[] = new long[ length + 1 ];
				long s = 0;
				for( int i = 0; i < value.length; i++ ) {
					cumulative[ i + 1 ] = cumulative[ i ] + ( value[ i ] = random.nextInt( length ) + 1 );
				}
				s = cumulative[ length ];
				//System.err.println("upper bound: " + ( s + 1 ) );
				efa.init( length, s, true, false, log2Quantum );
				for( int v: value ) efa.add( v );

				@SuppressWarnings("resource")
				FileOutputStream fos = new FileOutputStream( temp );
				QuasiSuccinctIndexWriter.LongWordOutputBitStream lwobs = new QuasiSuccinctIndexWriter.LongWordOutputBitStream( fos.getChannel(), ByteOrder.BIG_ENDIAN );

				final int l = efa.lowerBits();
				int skipPointersStart = 0;
				skipPointersStart += lwobs.writeGamma( l );
				if ( length >= ( 1 << log2Quantum ) ) skipPointersStart += lwobs.writeNonZeroGamma( efa.pointerSize() );
				efa.dump( lwobs );
				lwobs.close();

				long[] bits = BinIO.loadLongs( temp );
				QuasiSuccinctIndexReader.PositionReader quasiSuccinctPositionReader = 
						new PositionReader( LongBigArrayBigList.wrap( LongBigArrays.wrap( bits ) ), l, skipPointersStart, efa.numberOfPointers(), efa.pointerSize(), length, log2Quantum );
				
				/*LongArrayBitVector bv = LongArrayBitVector.wrap( bits );
				System.err.println( bv );
				System.err.println( Arrays.toString( value ) );
				System.err.println( Arrays.toString( cumulative ) );*/
				for( int i = 1; i < length - 2; i += 2 + random.nextInt( 2 ) ) {
					assertEquals( Integer.toString( i ), value[ i ] - 1, quasiSuccinctPositionReader.getFirstPosition( i ) );
					assertEquals( Integer.toString( i ), value[ i ] - 1 + value[ i + 1 ], quasiSuccinctPositionReader.getNextPosition() );
				}
			}
		}
		temp.delete();
	}
}

