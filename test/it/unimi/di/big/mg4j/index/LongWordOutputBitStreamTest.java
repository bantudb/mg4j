package it.unimi.di.big.mg4j.index;

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.util.XorShift128PlusRandom;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;

import org.junit.Test;


public class LongWordOutputBitStreamTest {
	@Test
	public void test() throws IOException {
		XorShift128PlusRandom random = new XorShift128PlusRandom( 0 );
		LongArrayBitVector v = LongArrayBitVector.getInstance();
		File temp = File.createTempFile( LongWordOutputBitStreamTest.class.getSimpleName(), "test" );
		temp.deleteOnExit();
		FileOutputStream fos = new FileOutputStream( temp );
		QuasiSuccinctIndexWriter.LongWordOutputBitStream lwobs = new QuasiSuccinctIndexWriter.LongWordOutputBitStream( fos.getChannel(), ByteOrder.BIG_ENDIAN );
		long l = 0;
		for( int i = 0; i < 1000; i++ ) {
			long value = random.nextLong();
			final int width = random.nextInt( Long.SIZE + 1 );
			if ( width != Long.SIZE ) value &= ( 1L << width ) - 1;
			v.append( value, width );
			l += lwobs.append( value, width );
		}
		l += lwobs.append(  v.bits(), v.length() );
		v.append( v );
		
		if ( l % Long.SIZE != 0 ) l += ( Long.SIZE - l % Long.SIZE );
		lwobs.close();
		DataInputStream dis = new DataInputStream( new FileInputStream( temp ) );
		for( int i = 0; i < l / Long.SIZE; i++ ) assertEquals( Integer.toString( i ) + " [" + l / Long.SIZE + "]", v.bits()[ i ], dis.readLong() );
		fos.close();
		dis.close();
		temp.delete();
	}
}
