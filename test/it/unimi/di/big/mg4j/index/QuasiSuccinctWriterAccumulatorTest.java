package it.unimi.di.big.mg4j.index;

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.util.XorShift128PlusRandom;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;


public class QuasiSuccinctWriterAccumulatorTest {
	// @Test ALERT
	public void test() throws IOException {
		XorShift128PlusRandom random = new XorShift128PlusRandom( 0 );
		File temp = File.createTempFile( QuasiSuccinctWriterAccumulatorTest.class.getSimpleName(), "test" );
		temp.deleteOnExit();

		for( boolean indexZeroes: new boolean[] { false, true } ) {
			for( boolean strict: new boolean[] { false, true } ) {
				for( int length: new int[] { 0, 1, 10, 100, 1000, 10000 } ) {
					for( int log2Quantum: new int[] { 0, 1, 2, 4, 8 } ) {
						System.err.println( "size: " + length + " log2Quantum: " + log2Quantum + " strict: " + strict + " indexZeroes: " + indexZeroes );
						QuasiSuccinctIndexWriter.Accumulator efa = new QuasiSuccinctIndexWriter.Accumulator( 1024 * 1024, log2Quantum );
						final int[] value = new int[ length ];
						long s = 0;
						for( int i = 0; i < value.length; i++ ) s += ( value[ i ] = random.nextInt( length ) + ( strict ? 1 : 0 ) );
						System.err.println("upper bound: " + s );
						efa.init( length, s, strict, indexZeroes, log2Quantum );
						for( int v: value ) efa.add( v );

						FileOutputStream fos = new FileOutputStream( temp );
						QuasiSuccinctIndexWriter.LongWordOutputBitStream lwobs = new QuasiSuccinctIndexWriter.LongWordOutputBitStream( fos.getChannel(), ByteOrder.BIG_ENDIAN );
						
						long numBits = efa.dump( lwobs );
						lwobs.close();
						efa.close();
						fos.close();
						if ( numBits % Long.SIZE != 0 ) numBits += Long.SIZE - numBits % Long.SIZE;

						long[] bits = new long[ (int)( numBits / Long.SIZE ) ];
						assertEquals( bits.length, BinIO.loadLongs( temp, bits ) );
						LongArrayBitVector bv = LongArrayBitVector.wrap( bits );

						// We know the number of lower bits from the upper bound and length.
						final int l = QuasiSuccinctIndexWriter.lowerBits( length, s, strict );
						// From that we obtain the pointer size.
						final int pointerSize = QuasiSuccinctIndexWriter.pointerSize( length, s, strict, indexZeroes );

						long currPointer = 0;
						// This skips to the lower bits.
						long currLower = QuasiSuccinctIndexWriter.numberOfPointers( length, s, log2Quantum, strict, indexZeroes ) * pointerSize;
						long startUpper = currLower + l * length, currUpper = startUpper; 
						long currPrefixSum = 0;
						long currZeroes = 0;
						long quantumMask = ( 1L << log2Quantum ) - 1;

						for( int i = 0; i < length; i++ ) {
							while( ! bv.getBoolean( currUpper++ ) ) {
								if ( indexZeroes && ( currZeroes + 1 & quantumMask ) == 0 ) assertEquals( Long.toString( currZeroes ), currUpper - startUpper, bv.getLong( currPointer, currPointer += pointerSize ) );
								currZeroes++;
							}
							long prefixSum = ( currZeroes << l ) + bv.getLong( currLower, currLower += l );
							assertEquals( Integer.toString( i ), value[ i ], prefixSum - currPrefixSum + ( strict ? 1 : 0 ) );
							if ( ! indexZeroes && ( i + 1 & quantumMask ) == 0 ) assertEquals( currUpper - startUpper, bv.getLong( currPointer, currPointer += pointerSize ) );
							currPrefixSum = prefixSum;
						}
					}
				}
			}
		}
		temp.delete();
	}
}

