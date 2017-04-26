package it.unimi.di.big.mg4j.index;

/*		 
 * MG4J: Managing Gigabytes for Java
 *
 * Copyright (C) 2004-2016 Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.index.Index.UriKeys;
import it.unimi.di.big.mg4j.index.QuasiSuccinctIndex.PropertyKeys;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.IOFactories;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.util.SemiExternalOffsetBigList;
import it.unimi.dsi.big.util.PrefixMap;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.ints.AbstractIntBigList;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntBigArrayBigList;
import it.unimi.dsi.fastutil.ints.IntBigArrays;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.longs.LongBigLists;
import it.unimi.dsi.io.ByteBufferInputStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.sux4j.util.EliasFanoLongBigList;
import it.unimi.dsi.util.ByteBufferLongBigList;
import it.unimi.dsi.util.Properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;
import java.util.EnumMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/** A static container providing facilities to load an index based on data stored on disk.
 *
 * <P>This class contains several useful static methods 
 * such as {@link #readOffsets(InputBitStream, long)}, {@link #readSizes(CharSequence, long)}, {@link #loadLongBigList(CharSequence, ByteOrder)}
 * and static factor methods such as {@link #getInstance(CharSequence, boolean, boolean, boolean, EnumMap)}
 * that take care of reading the properties associated with the index, identify
 * the correct {@link it.unimi.di.big.mg4j.index.Index} implementation that
 * should be used to load the index, and load the necessary data into memory. 
 * 
 * <p>As an option, a disk-based index can be <em>loaded</em> into main memory (key: {@link Index.UriKeys#INMEMORY}),
 * or <em>mapped</em> into main memory (key: {@link Index.UriKeys#MAPPED}) (the value assigned to the keys is irrelevant).
 *  
 * <p>Note that {@linkplain QuasiSuccinctIndex quasi-succinct indices} are memory-mapped by default,
 * and for {@linkplain BitStreamIndex bitstream indices} there is a limit of two gigabytes 
 * for in-memory indices.
 * 
 * <p>By default the
 * term-offset list is accessed using a {@link it.unimi.di.big.mg4j.util.SemiExternalOffsetBigList}
 * with a step of {@link #DEFAULT_OFFSET_STEP}. This behaviour can be changed using
 * the URI key {@link UriKeys#OFFSETSTEP}.
 * 
 * <p>Disk-based indices are the workhorse of MG4J. All other indices (clustered,
 * remote, etc.) ultimately rely on disk-based indices to provide results.
 * 
 * <p>Note that not all data produced by {@link it.unimi.di.big.mg4j.tool.Scan} and
 * by the other indexing utilities are actually necessary to run a disk-based
 * index. Usually the property file and the index files are sufficient: if one
 * needs random access, also the offsets file must be present, and if the
 * compression method requires document sizes or if sizes are requested explicitly,
 * also the sizes file must be present. A {@link StringMap}
 * and possibly a {@link PrefixMap} will be fetched
 * automatically by {@link #getInstance(CharSequence, boolean, boolean)}
 * using standard extensions.
 *
 * <h2>Thread safety</h2>
 * 
 * <p>A disk-based index is thread safe as long as the offset list, the size list and
 * the term/prefix map are. The static factory methods provided by this class load
 * offsets and sizes using data structures that are thread safe. If you use directly
 * a constructor, instead, it is your responsibility to pass thread-safe data structures.
 *
 * @author Sebastiano Vigna
 * @since 1.1
 */

public class DiskBasedIndex {
	private static final Logger LOGGER = LoggerFactory.getLogger( DiskBasedIndex.class );

	/** The default value for the query parameter {@link Index.UriKeys#OFFSETSTEP}. */
	public final static int DEFAULT_OFFSET_STEP = 256;

	/** Standard extension for the index bitstream. */
	public static final String INDEX_EXTENSION = ".index";
	/** Standard extension for the positions bitstream of a {@linkplain BitStreamHPIndexWriter high-performance index}. */
	public static final String POSITIONS_EXTENSION = ".positions";
	/** Standard extension for the index properties. */
	public static final String PROPERTIES_EXTENSION = ".properties";
	/** Standard extension for the file of sizes. */
	public static final String SIZES_EXTENSION = ".sizes";
	/** Standard extension for the file of offsets. */
	public static final String OFFSETS_EXTENSION = ".offsets";
	/** Standard extension for the file of lengths of positions. */
	public static final String POSITIONS_NUMBER_OF_BITS_EXTENSION = ".posnumbits";
	/** Standard extension for the file of lengths of positions. */
	public static final String SUMS_MAX_POSITION_EXTENSION = ".sumsmaxpos";
	/** Standard extension for the file of global counts. */
	public static final String OCCURRENCIES_EXTENSION = ".occurrencies";
	/** Standard extension for the file of frequencies. */
	public static final String FREQUENCIES_EXTENSION = ".frequencies";
	/** Standard extension for the file of terms. */
	public static final String TERMS_EXTENSION = ".terms";
	/** Standard extension for the file of terms, unsorted. */
	public static final String UNSORTED_TERMS_EXTENSION = ".terms.unsorted";
	/** Standard extension for the term map. */
	public static final String TERMMAP_EXTENSION = ".termmap";
	/** Standard extension for the prefix map. */
	public static final String PREFIXMAP_EXTENSION = ".prefixmap";
	/** Standard extension for the stats file. */
	public static final String STATS_EXTENSION = ".stats";
	/** The extension for the pointers bitstream. */
	public static final String POINTERS_EXTENSIONS = ".pointers";
	/** The extension for the counts bitstream. */
	public static final String COUNTS_EXTENSION = ".counts";
	/** The postfix to be added to {@link #POINTERS_EXTENSIONS}, {@link #COUNTS_EXTENSION} and {@link #POSITIONS_EXTENSION} for offsets. */
	public static final String OFFSETS_POSTFIX = "offsets";
	/** The size of the buffer used by {@link #loadLongBigList(ReadableByteChannel, long, ByteOrder)}. */
	public static final int BUFFER_SIZE = 64 * 1024;
	
	private DiskBasedIndex() {}
	
	/** Utility method to load a compressed offset file into a list.
	 *
	 * @param in the input bit stream providing the offsets (see {@link BitStreamIndexWriter}).
	 * @param T the number of terms indexed.
	 * @return a list of longs backed by an array; the list has
	 * an additional final element of index <code>T</code> that gives the number
	 * of bytes of the index file.
	 */

	public static LongBigList readOffsets( final InputBitStream in, final long T ) throws IOException {
		final long[][] offset = LongBigArrays.newBigArray( T + 1 );
		LOGGER.debug( "Loading offsets..." );
		long prev;
		LongBigArrays.set( offset, 0, prev = in.readLongGamma() );
		for( int i = 0; i < T; i++ ) LongBigArrays.set( offset, i + 1, prev = in.readLongGamma() + prev );
		LOGGER.debug( "Completed." );
		return LongBigArrayBigList.wrap( offset );
	}

	/** Utility method to load a compressed offset file into a list.
	 *
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param filename the file containing the offsets (see {@link BitStreamIndexWriter}).
	 * @param T the number of terms indexed.
	 * @return a list of longs backed by an array; the list has
	 * an additional final element of index <code>T</code> that gives the number
	 * of bytes of the index file.
	 */

	public static LongBigList readOffsets( final IOFactory ioFactory, final CharSequence filename, final long T ) throws IOException {
		final InputBitStream in = new InputBitStream( ioFactory.getInputStream( filename.toString() ), false );
		final LongBigList offsets = readOffsets( in, T );
		in.close();
		return offsets;
	}

	/** Utility method to load a compressed offset file into a list using the {@link IOFactory#FILESYSTEM_FACTORY}.
	 *
	 * @param filename the file containing the offsets (see {@link BitStreamIndexWriter}).
	 * @param T the number of terms indexed.
	 * @return a list of longs backed by an array; the list has
	 * an additional final element of index <code>T</code> that gives the number
	 * of bytes of the index file.
	 */

	public static LongBigList readOffsets( final CharSequence filename, final long T ) throws IOException {
		return readOffsets( IOFactory.FILESYSTEM_FACTORY, filename, T );
	}
	
	/** Utility method to load a compressed size file into a list.
	 *
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param filename the file containing the &gamma;-coded sizes (see {@link BitStreamIndexWriter}).
	 * @param n the number of documents.
	 * @return a list of integers backed by an array.
	 */

	public static IntBigArrayBigList readSizes( final IOFactory ioFactory, final CharSequence filename, final long n ) throws IOException {
		final int[][] size = IntBigArrays.newBigArray( n );
		final InputBitStream in = new InputBitStream( ioFactory.getInputStream( filename.toString() ), false );
		LOGGER.debug( "Loading sizes..." );
		for( int segment = 0; segment < size.length; segment++ ) in.readGammas( size[ segment ], size[ segment ].length );		  
		LOGGER.debug( "Completed." );
		in.close();
		return IntBigArrayBigList.wrap( size );
	}

	/** Utility method to load a compressed size file into a list using the {@link IOFactory#FILESYSTEM_FACTORY}.
	 *
	 * @param filename the file containing the &gamma;-coded sizes (see {@link BitStreamIndexWriter}).
	 * @param N the number of documents.
	 * @return a list of integers backed by an array.
	 */

	public static IntBigArrayBigList readSizes( final CharSequence filename, final long N ) throws IOException {
		return readSizes( IOFactory.FILESYSTEM_FACTORY, filename, N );
	}

	/** Utility method to load a compressed size file into an {@linkplain EliasFanoLongBigList Elias&ndash;Fano compressed list}.
	 *
	 * @param filename the filename containing the &gamma;-coded sizes (see {@link BitStreamIndexWriter}).
	 * @param N the number of documents indexed.
	 * @return a list of integers backed by an {@linkplain EliasFanoLongBigList Elias&ndash;Fano compressed list}.
	 * @throws IllegalStateException if <code>ioFactory</code> is not {@link IOFactory#FILESYSTEM_FACTORY}.
	 * 
	 * @deprecated This method is an ancestral residue.
	 */

	@Deprecated
	public static IntBigList readSizesSuccinct( final CharSequence filename, final long N ) throws IOException {
		LOGGER.debug( "Loading sizes..." );
		final IntBigList sizes = new AbstractIntBigList() {
			final EliasFanoLongBigList list = new EliasFanoLongBigList( new GammaCodedIterableList( BinIO.loadBytes( filename ), N ) );

			public int getInt( long index ) {
				return (int)list.getLong( index );
			}

			public long size64() {
				return list.size64();
			}
		};
		LOGGER.debug( "Completed." );
		return sizes;
	}

	// TODO: replace this with a general-purpose class
	private static class GammaCodedIterableList implements IntIterable {
		protected final long n;
		protected final byte[] array;

		public GammaCodedIterableList( final byte []array, final long n ) {
			this.array = array;
			this.n = n;
		}

		public IntIterator iterator() {
			return new AbstractIntIterator() {
				final InputBitStream ibs = new InputBitStream( array );
				long pos;
				
				public boolean hasNext() {
					return pos < n;
				}
				
				public int nextInt() {
					if ( ! hasNext() ) throw new NoSuchElementException();
					pos++;
					try {
						return ibs.readGamma();
					}
					catch ( IOException e ) {
						throw new RuntimeException( e );
					} 
				}
			};
		}
	}
	
	/** Commodity method for loading a big list of binary longs with specified endianness into a {@linkplain LongBigArrays long big array}.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param filename the file containing the longs.
	 * @param byteOrder the endianness of the longs.
	 * @return a big list of longs containing the longs in <code>file</code>.
	 */
	public static LongBigArrayBigList loadLongBigList( final IOFactory ioFactory, final CharSequence filename, final ByteOrder byteOrder ) throws IOException {
		final long length = ioFactory.length( filename.toString() ) / ( Long.SIZE / Byte.SIZE );
		ReadableByteChannel channel = ioFactory.getReadableByteChannel( filename.toString() );
		final LongBigArrayBigList loadLongBigList = loadLongBigList( channel, length, byteOrder );
		channel.close();
		return loadLongBigList;

	}
	
	/** Commodity method for loading a big list of binary longs with specified endianness into a {@linkplain LongBigArrays long big array} using the {@link IOFactory#FILESYSTEM_FACTORY}.
	 * 
	 * @param filename the file containing the longs.
	 * @param byteOrder the endianness of the longs.
	 * @return a big list of longs containing the longs in <code>file</code>.
	 */
	public static LongBigArrayBigList loadLongBigList( final CharSequence filename, final ByteOrder byteOrder ) throws IOException {
		return loadLongBigList( IOFactory.FILESYSTEM_FACTORY, filename, byteOrder );
	}
	
	/** Commodity method for loading from a channel a big list of binary longs with specified endianness into a {@linkplain LongBigArrays long big array}.
	 * 
	 * @param channel the channel.
	 * @param byteOrder the endianness of the longs.
	 * @return a big list of longs containing the longs returned by <code>channel</code>.
	 */
	public static LongBigArrayBigList loadLongBigList( final ReadableByteChannel channel, final long length, final ByteOrder byteOrder ) throws IOException {
		final ByteBuffer byteBuffer = ByteBuffer.allocateDirect( BUFFER_SIZE ).order( byteOrder );
		
		LongBigArrayBigList list = new LongBigArrayBigList( length );
		
		while( channel.read( byteBuffer ) > 0 ) {
			byteBuffer.flip();
			while( byteBuffer.hasRemaining() ) list.add( byteBuffer.getLong() );
			byteBuffer.clear();
		}
		
		return list;
	}
	

	/** Parses a {@link ByteOrder} value.
	 * 
	 * @param s a string (either <samp>BIG_ENDIAN</samp> or <samp>LITTLE_ENDIAN</samp>).
	 * @return the corresponding byte order ({@link ByteOrder#BIG_ENDIAN} or {@link ByteOrder#LITTLE_ENDIAN}).
	 */
	public static ByteOrder byteOrder( final String s ) {
		if ( s.equals( ByteOrder.BIG_ENDIAN.toString() ) ) return ByteOrder.BIG_ENDIAN;
		if ( s.equals( ByteOrder.LITTLE_ENDIAN.toString() ) ) return ByteOrder.LITTLE_ENDIAN;
		throw new IllegalArgumentException( "Unknown byte order " + s );
	}

	
	/** Utility static method that loads a term map.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param filename the name of the file containing the term map.
	 * @return the map, or <code>null</code> if the file did not exist.
	 * @throws IOException if some IOException (other than {@link FileNotFoundException}) occurred.
	 */
	@SuppressWarnings("unchecked")
	public static StringMap<? extends CharSequence> loadStringMap( final IOFactory ioFactory, final String filename ) throws IOException {
		try {
			return (StringMap<? extends CharSequence>) IOFactories.loadObject( ioFactory, filename );
		} catch ( FileNotFoundException e ) {
			return null;
		} catch ( ClassNotFoundException e ) {
			throw new RuntimeException( e );
		}
	}

	/** Utility static method that loads a term map using the {@link IOFactory#FILESYSTEM_FACTORY}.
	 * 
	 * @param filename the name of the file containing the term map.
	 * @return the map, or <code>null</code> if the file did not exist.
	 * @throws IOException if some IOException (other than {@link FileNotFoundException}) occurred.
	 */
	public static StringMap<? extends CharSequence> loadStringMap( final String filename ) throws IOException {
		return loadStringMap( IOFactory.FILESYSTEM_FACTORY, filename );
	}

	/** Utility static method that loads a prefix map.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param filename the name of the file containing the prefix map.
	 * @return the map, or <code>null</code> if the file did not exist.
	 * @throws IOException if some IOException (other than {@link FileNotFoundException}) occurred.
	 */
	@SuppressWarnings("unchecked")
	public static PrefixMap<? extends CharSequence> loadPrefixMap( final IOFactory ioFactory, final String filename ) throws IOException {
		try {
			return  (PrefixMap<? extends CharSequence>) IOFactories.loadObject( ioFactory, filename );
		} catch ( FileNotFoundException e ) {
			return null;
		} catch ( ClassNotFoundException e ) {
			throw new RuntimeException( e );
		}
	}

	/** Utility static method that loads a prefix map using the {@link IOFactory#FILESYSTEM_FACTORY}.
	 * 
	 * @param filename the name of the file containing the prefix map.
	 * @return the map, or <code>null</code> if the file did not exist.
	 * @throws IOException if some IOException (other than {@link FileNotFoundException}) occurred.
	 */
	public static PrefixMap<? extends CharSequence> loadPrefixMap( final String filename ) throws IOException {
		return loadPrefixMap( IOFactory.FILESYSTEM_FACTORY, filename );
	}

	
	/** Returns the list of offsets.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param filename the file containing the offsets.
	 * @param numberOfTerms the number of terms.
	 * @param offsetStep the offset step.  
	 * @return if <code>offsetStep</code> is less than zero, a memory-mapped,
	 * synchronized {@link SemiExternalOffsetBigList} with offset step
	 * equal to <code>-offsetStep</code>; if it is zero, an
	 * in-memory list; if it is greater than than zero, 
	 * we return a synchronized {@link SemiExternalOffsetBigList} with offset step
	 * equal to <code>-offsetStep</code>.
	 */
	@SuppressWarnings("resource")
	public static LongBigList offsets( final IOFactory ioFactory, final String filename, final long numberOfTerms, int offsetStep ) throws FileNotFoundException, IOException {
		final LongBigList offsets;
		if ( offsetStep != 0 && ioFactory != IOFactory.FILESYSTEM_FACTORY ) throw new IllegalArgumentException( "Memory-mapped and on-disk offsets are available only using the file system I/O factory." );
		if ( offsetStep < 0 ) { // Memory-mapped
			offsetStep  = -offsetStep;
			offsets = LongBigLists.synchronize( new SemiExternalOffsetBigList( 
					new InputBitStream( ByteBufferInputStream.map( new FileInputStream( filename ).getChannel(), MapMode.READ_ONLY ) ),
					offsetStep, numberOfTerms + 1 ) );
		}
		else {
			offsets = offsetStep == 0? 
					DiskBasedIndex.readOffsets( filename, numberOfTerms ) :
						LongBigLists.synchronize( new SemiExternalOffsetBigList( new InputBitStream( filename, 1024 ), offsetStep, numberOfTerms + 1 ) );
		}

		if ( offsets.size64() != numberOfTerms + 1 ) throw new IllegalStateException( "The length of the offset list (" + offsets.size64() + ") is not equal to the number of terms plus one (" + numberOfTerms + " + 1)" );
		return offsets;	
	}

	/** Returns the list of offsets using the {@link IOFactory#FILESYSTEM_FACTORY}. 
	 * 
	 * @param filename the file containing the offsets.
	 * @param numberOfTerms the number of terms.
	 * @param offsetStep the offset step.  
	 * @return if <code>offsetStep</code> is less than zero, a memory-mapped,
	 * synchronized {@link SemiExternalOffsetBigList} with offset step
	 * equal to <code>-offsetStep</code>; if it is zero, an
	 * in-memory list; if it is greater than than zero, 
	 * we return a synchronized {@link SemiExternalOffsetBigList} with offset step
	 * equal to <code>-offsetStep</code>.
	 */
	public static LongBigList offsets( final String filename, final long numberOfTerms, int offsetStep ) throws FileNotFoundException, IOException {
		return offsets( IOFactory.FILESYSTEM_FACTORY, filename, numberOfTerms, offsetStep );
	}

	/** Returns a new disk-based index, loading exactly the specified parts and using preloaded {@link Properties}.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param basename the basename of the index.
	 * @param properties the properties obtained from the given basename.
	 * @param termMap the term map for this index, or <code>null</code> for no term map.
	 * @param prefixMap the prefix map for this index, or <code>null</code> for no prefix map.
	 * @param randomAccess whether the index should be accessible randomly (e.g., if it will
	 * be possible to call {@link IndexReader#documents(long)} on the index readers returned by the index).
	 * @param documentSizes if true, document sizes will be loaded (note that sometimes document sizes
	 * might be loaded anyway because the compression method for positions requires it).
	 * @param queryProperties a map containing associations between {@link Index.UriKeys} and values, or <code>null</code>.
	 */
	@SuppressWarnings("resource")
	public static Index getInstance( final IOFactory ioFactory, final CharSequence basename, Properties properties, final StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, final boolean randomAccess, final boolean documentSizes, final EnumMap<UriKeys,String> queryProperties ) throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {

		// This could be null if old indices contain SkipIndex
		Class<?> indexClass = null;
		try {
			// Compatibility with previous versions
			indexClass = Class.forName( properties.getString( Index.PropertyKeys.INDEXCLASS, "(missing index class)" ).replace( ".dsi.", ".di." ) );
		}
		catch( Exception ignore ) {}

		final long numberOfDocuments = properties.getLong( Index.PropertyKeys.DOCUMENTS ); 
		final long numberOfTerms = properties.getLong( Index.PropertyKeys.TERMS );
		final long numberOfPostings= properties.getLong( Index.PropertyKeys.POSTINGS ); 
		final long numberOfOccurrences = properties.getLong( Index.PropertyKeys.OCCURRENCES, -1 );
		final int maxCount = properties.getInt( Index.PropertyKeys.MAXCOUNT, -1 );
		final String field = properties.getString( Index.PropertyKeys.FIELD, new File( basename.toString() ).getName() );

		if ( termMap != null && termMap.size64() != numberOfTerms ) throw new IllegalArgumentException( "The size of the term map (" + termMap.size64() + ") is not equal to the number of terms (" + numberOfTerms + ")" );
		if ( prefixMap != null && prefixMap.size64() != numberOfTerms ) throw new IllegalArgumentException( "The size of the prefix map (" + prefixMap.size64() + ") is not equal to the number of terms (" + numberOfTerms + ")" );

		final Payload payload = (Payload)( properties.containsKey( Index.PropertyKeys.PAYLOADCLASS ) ? Class.forName( properties.getString( Index.PropertyKeys.PAYLOADCLASS ) ).newInstance() : null );

		final int skipQuantum = properties.getInt( BitStreamIndex.PropertyKeys.SKIPQUANTUM, -1 );
		
		final int bufferSize = properties.getInt( BitStreamIndex.PropertyKeys.BUFFERSIZE, BitStreamIndex.DEFAULT_BUFFER_SIZE );
		final int offsetStep = queryProperties != null && queryProperties.get( UriKeys.OFFSETSTEP ) != null ? Integer.parseInt( queryProperties.get( UriKeys.OFFSETSTEP ) ) : DEFAULT_OFFSET_STEP;

		final boolean highPerformance = indexClass != null && FileHPIndex.class.isAssignableFrom( indexClass );
		final boolean inMemory = queryProperties != null && queryProperties.containsKey( UriKeys.INMEMORY );
		final TermProcessor termProcessor = Index.getTermProcessor( properties );

		// Load document sizes if forced to do so, or if the pointer/position compression methods make it necessary.
		IntBigList sizes = null;
		
		if ( queryProperties != null && queryProperties.containsKey( UriKeys.SUCCINCTSIZES ) && ioFactory != IOFactory.FILESYSTEM_FACTORY ) throw new IllegalArgumentException( "Succinct sizes are deprecated and available only using the file system I/O factory." );
		
		if ( QuasiSuccinctIndex.class == indexClass ) {
			if ( ioFactory != IOFactory.FILESYSTEM_FACTORY && ! inMemory ) throw new IllegalArgumentException( "Memory-mapped quasi-succinct indices require the file system I/O factory." );
			final Map<Component,Coding> flags = CompressionFlags.valueOf( properties.getStringArray( Index.PropertyKeys.CODING ), CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX );
			final File pointersFile = new File( basename + POINTERS_EXTENSIONS );
			if ( ! pointersFile.exists() ) throw new FileNotFoundException( "Cannot find pointers file " + pointersFile.getName() );

			if ( documentSizes ) {
				sizes = queryProperties != null && queryProperties.containsKey( UriKeys.SUCCINCTSIZES ) ? readSizesSuccinct( basename + DiskBasedIndex.SIZES_EXTENSION, numberOfDocuments ) : readSizes( ioFactory, basename + DiskBasedIndex.SIZES_EXTENSION, numberOfDocuments );
				if ( sizes.size64() != numberOfDocuments ) throw new IllegalStateException( "The length of the size list (" + sizes.size64() + ") is not equal to the number of documents (" + numberOfDocuments + ")" );
			}

			final ByteOrder byteOrder = byteOrder( properties.getString( PropertyKeys.BYTEORDER ) );
			final boolean hasCounts = flags.containsKey( Component.COUNTS );
			final boolean hasPositions = flags.containsKey( Component.POSITIONS );
			return new QuasiSuccinctIndex( 
					inMemory ? loadLongBigList( ioFactory, basename + POINTERS_EXTENSIONS, byteOrder ) : ByteBufferLongBigList.map( new FileInputStream( basename + POINTERS_EXTENSIONS ).getChannel(), byteOrder, MapMode.READ_ONLY ),
					hasCounts ? ( inMemory ? loadLongBigList( ioFactory, basename + COUNTS_EXTENSION, byteOrder ) : ByteBufferLongBigList.map( new FileInputStream( basename + COUNTS_EXTENSION ).getChannel(), byteOrder, MapMode.READ_ONLY ) ) : null,
					hasPositions ? ( inMemory ? loadLongBigList( ioFactory, basename + POSITIONS_EXTENSION, byteOrder ) : ByteBufferLongBigList.map( new FileInputStream( basename + POSITIONS_EXTENSION ).getChannel(), byteOrder, MapMode.READ_ONLY ) ) : null,
							 numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, payload, Fast.mostSignificantBit( skipQuantum ), hasCounts, hasPositions,
								Index.getTermProcessor( properties ), field, properties, termMap, prefixMap, sizes,
								DiskBasedIndex.offsets( ioFactory, basename + POINTERS_EXTENSIONS + OFFSETS_POSTFIX, numberOfTerms, offsetStep ),
								  hasCounts ? DiskBasedIndex.offsets( ioFactory, basename + COUNTS_EXTENSION + OFFSETS_POSTFIX, numberOfTerms, offsetStep ) : null,
								   hasPositions ? DiskBasedIndex.offsets( ioFactory, basename + POSITIONS_EXTENSION + OFFSETS_POSTFIX, numberOfTerms, offsetStep ) : null );

		}

		final Map<Component,Coding> flags = CompressionFlags.valueOf( properties.getStringArray( Index.PropertyKeys.CODING ), null );

		final Coding frequencyCoding = flags.get( Component.FREQUENCIES );
		final Coding pointerCoding = flags.get( Component.POINTERS );
		final Coding countCoding = flags.get( Component.COUNTS );
		final Coding positionCoding = flags.get( Component.POSITIONS );
		if ( countCoding == null && positionCoding != null ) throw new IllegalArgumentException( "Index " + basename + " has positions but no counts (this can't happen)" );

		if ( payload == null && ( documentSizes || positionCoding == Coding.GOLOMB || positionCoding == Coding.INTERPOLATIVE ) ) {
			sizes = queryProperties != null && queryProperties.containsKey( UriKeys.SUCCINCTSIZES ) ? readSizesSuccinct( basename + DiskBasedIndex.SIZES_EXTENSION, numberOfDocuments ) : readSizes( basename + DiskBasedIndex.SIZES_EXTENSION, numberOfDocuments );
			if ( sizes.size64() != numberOfDocuments ) throw new IllegalStateException( "The length of the size list (" + sizes.size64() + ") is not equal to the number of documents (" + numberOfDocuments + ")" );
		}

		final int height = properties.getInt( BitStreamIndex.PropertyKeys.SKIPHEIGHT, -1 );
		// Load offsets if forced to do so. Depending on a property, we use the core-memory or the semi-external version.
		final LongBigList offsets = payload == null && randomAccess ? offsets( ioFactory, basename + OFFSETS_EXTENSION, numberOfTerms, offsetStep ) : null;

		final String indexFile = basename + INDEX_EXTENSION ;
		if ( ! ioFactory.exists( indexFile ) ) throw new FileNotFoundException( "Cannot find index file " + indexFile );

		if ( inMemory ) {
			/*if ( SqrtSkipIndex.class.isAssignableFrom( indexClass ) )
				return new SqrtSkipInMemoryIndex( BinIO.loadBytes( indexFile.toString() ), 
						numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, 
						frequencyCoding, pointerCoding, countCoding, positionCoding,
						termProcessor,
						field, properties, termMap, prefixMap, sizes, offsets );*/
			return highPerformance ?
					new InMemoryHPIndex( IOFactories.loadBytes( ioFactory, indexFile ), IOFactories.loadBytes( ioFactory, basename + POSITIONS_EXTENSION ), 
					numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, 
					payload, frequencyCoding, pointerCoding, countCoding, positionCoding, skipQuantum, height,
					termProcessor,
					field, properties, termMap, prefixMap, sizes, offsets )
			: new InMemoryIndex( IOFactories.loadBytes( ioFactory, indexFile.toString() ), 
					numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, 
					payload, frequencyCoding, pointerCoding, countCoding, positionCoding, skipQuantum, height,
					termProcessor,
					field, properties, termMap, prefixMap, sizes, offsets );
		}
		else if ( queryProperties != null && queryProperties.containsKey( UriKeys.MAPPED ) ) {
			if ( ioFactory != IOFactory.FILESYSTEM_FACTORY ) throw new IllegalArgumentException( "Mapped indices require the file system I/O factory." );
			final File positionsFile = new File( basename + POSITIONS_EXTENSION );
			final ByteBufferInputStream index = ByteBufferInputStream.map( new FileInputStream( indexFile ).getChannel(), MapMode.READ_ONLY );
			return highPerformance 
					? new MemoryMappedHPIndex( index, ByteBufferInputStream.map( new FileInputStream( positionsFile ).getChannel(), MapMode.READ_ONLY ),
					numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, 
					payload, frequencyCoding, pointerCoding, countCoding, positionCoding, skipQuantum, height,
					termProcessor,
					field, properties, termMap, prefixMap, sizes, offsets )
					: new MemoryMappedIndex( index,
							numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, 
							payload, frequencyCoding, pointerCoding, countCoding, positionCoding, skipQuantum, height,
							termProcessor,
							field, properties, termMap, prefixMap, sizes, offsets );
			
		}
		/*if ( SqrtSkipIndex.class.isAssignableFrom( indexClass ) )
			return new SqrtSkipFileIndex( basename.toString(), 
				numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, 
				frequencyCoding, pointerCoding, countCoding, positionCoding,
				termProcessor,
				field, properties, termMap, prefixMap, sizes, offsets, indexFile );*/
		
		return highPerformance  
				? new FileHPIndex( basename.toString(), 
						numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, 
						payload, frequencyCoding, pointerCoding, countCoding, positionCoding, skipQuantum, height, bufferSize,
						termProcessor,
						field, properties, termMap, prefixMap, sizes, offsets )
				: new FileIndex( ioFactory, basename.toString(), 
				numberOfDocuments, numberOfTerms, numberOfPostings, numberOfOccurrences, maxCount, 
				payload, frequencyCoding, pointerCoding, countCoding, positionCoding, skipQuantum, height, bufferSize,
				termProcessor,
				field, properties, termMap, prefixMap, sizes, offsets );
		 
	}

	/** Returns a new disk-based index, loading exactly the specified parts and using preloaded {@link Properties} and the {@link IOFactory#FILESYSTEM_FACTORY}.
	 * 
	 * @param basename the basename of the index.
	 * @param properties the properties obtained from the given basename.
	 * @param termMap the term map for this index, or <code>null</code> for no term map.
	 * @param prefixMap the prefix map for this index, or <code>null</code> for no prefix map.
	 * @param randomAccess whether the index should be accessible randomly (e.g., if it will
	 * be possible to call {@link IndexReader#documents(long)} on the index readers returned by the index).
	 * @param documentSizes if true, document sizes will be loaded (note that sometimes document sizes
	 * might be loaded anyway because the compression method for positions requires it).
	 * @param queryProperties a map containing associations between {@link Index.UriKeys} and values, or <code>null</code>.
	 */
	public static Index getInstance( final CharSequence basename, Properties properties, final StringMap<? extends CharSequence> termMap, final PrefixMap<? extends CharSequence> prefixMap, final boolean randomAccess, final boolean documentSizes, final EnumMap<UriKeys,String> queryProperties ) throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		return getInstance( IOFactory.FILESYSTEM_FACTORY, basename, properties, termMap, prefixMap, randomAccess, documentSizes, queryProperties );
	}
	
	/** Returns a new disk-based index, using preloaded {@link Properties} and possibly guessing reasonable term and prefix maps from the basename.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param basename the basename of the index.
	 * @param properties the properties obtained by stemming <code>basename</code>.
	 * @param randomAccess whether the index should be accessible randomly.
	 * @param documentSizes if true, document sizes will be loaded.
	 * @param maps if true, {@linkplain StringMap term} and {@linkplain PrefixMap prefix} maps will be guessed and loaded.
	 * @param queryProperties a map containing associations between {@link Index.UriKeys} and values, or <code>null</code>.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * 
	 * @see #getInstance(CharSequence, Properties, StringMap, PrefixMap, boolean, boolean, EnumMap)
	 */
	public static Index getInstance( final IOFactory ioFactory, final CharSequence basename, final Properties properties, final boolean randomAccess, final boolean documentSizes, final boolean maps, final EnumMap<UriKeys,String> queryProperties ) throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		StringMap<? extends CharSequence> termMap = null;
		PrefixMap<? extends CharSequence> prefixMap = null;
		if ( maps ) {
			termMap = DiskBasedIndex.loadStringMap( ioFactory, basename + DiskBasedIndex.TERMMAP_EXTENSION );
			if ( termMap != null && termMap instanceof PrefixMap ) return getInstance( ioFactory, basename, properties, termMap, (PrefixMap<?>)termMap, randomAccess, documentSizes, queryProperties );
			prefixMap = DiskBasedIndex.loadPrefixMap( ioFactory, basename + DiskBasedIndex.PREFIXMAP_EXTENSION );
			if ( termMap != null ) return getInstance( ioFactory, basename, properties, termMap, prefixMap, randomAccess, documentSizes, queryProperties );
			if ( prefixMap != null ) return getInstance( ioFactory, basename, properties, prefixMap, prefixMap, randomAccess, documentSizes, queryProperties );
		}
		return getInstance( ioFactory, basename, properties, null, prefixMap, randomAccess, documentSizes, queryProperties );
	}

	/** Returns a new disk-based index, using preloaded {@link Properties} and possibly guessing reasonable term and prefix maps from the basename.
	 * 
	 * @param basename the basename of the index.
	 * @param properties the properties obtained by stemming <code>basename</code>.
	 * @param randomAccess whether the index should be accessible randomly.
	 * @param documentSizes if true, document sizes will be loaded.
	 * @param maps if true, {@linkplain StringMap term} and {@linkplain PrefixMap prefix} maps will be guessed and loaded.
	 * @param queryProperties a map containing associations between {@link Index.UriKeys} and values, or <code>null</code>.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * 
	 * @see #getInstance(CharSequence, Properties, StringMap, PrefixMap, boolean, boolean, EnumMap)
	 */
	public static Index getInstance( final CharSequence basename, final Properties properties, final boolean randomAccess, final boolean documentSizes, final boolean maps, final EnumMap<UriKeys,String> queryProperties ) throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		return getInstance( IOFactory.FILESYSTEM_FACTORY, basename, properties, randomAccess, documentSizes, maps, queryProperties );
	}


	/** Returns a new disk-based index, possibly guessing reasonable term and prefix maps from the basename.
	 * 
	 * <p>If there is a term map file (basename stemmed with <samp>.termmap</samp>), it is used as term map and,
	 * in case it implements {@link PrefixMap}. Otherwise, we search for a prefix map (basename stemmed with <samp>.prefixmap</samp>)
	 * and, if it implements {@link StringMap} and no term map has been found, we use it as prefix map.
	 * 
	 * @param basename the basename of the index.
	 * @param randomAccess whether the index should be accessible randomly (e.g., if it will
	 * be possible to call {@link IndexReader#documents(long)} on the index readers returned by the index).
	 * @param documentSizes if true, document sizes will be loaded (note that sometimes document sizes
	 * might be loaded anyway because the compression method for positions requires it).
	 * @param maps if true, {@linkplain StringMap term} and {@linkplain PrefixMap prefix} maps will be guessed and loaded (this
	 * feature might not be available with some kind of index). 
	 * @param queryProperties a map containing associations between {@link Index.UriKeys} and values, or <code>null</code>.
	 */
	public static Index getInstance( final CharSequence basename, final boolean randomAccess, final boolean documentSizes, final boolean maps, final EnumMap<UriKeys,String> queryProperties ) throws ConfigurationException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		return getInstance( basename, new Properties( basename + DiskBasedIndex.PROPERTIES_EXTENSION ), randomAccess, documentSizes, maps, queryProperties );
	}


	/** Returns a new disk-based index, using preloaded {@link Properties} and possibly guessing reasonable term and prefix maps from the basename.
	 * 
	 * <p>If there is a term map file (basename stemmed with <samp>.termmap</samp>), it is used as term map and,
	 * in case it implements {@link PrefixMap}. Otherwise, we search for a prefix map (basename stemmed with <samp>.prefixmap</samp>)
	 * and, if it implements {@link StringMap} and no term map has been found, we use it as prefix map.
	 * 
	 * @param basename the basename of the index.
	 * @param randomAccess whether the index should be accessible randomly (e.g., if it will
	 * be possible to call {@link IndexReader#documents(long)} on the index readers returned by the index).
	 * @param documentSizes if true, document sizes will be loaded (note that sometimes document sizes
	 * might be loaded anyway because the compression method for positions requires it).
	 * @param maps if true, {@linkplain StringMap term} and {@linkplain PrefixMap prefix} maps will be guessed and loaded (this
	 * feature might not be available with some kind of index).
	 * @see #getInstance(CharSequence, boolean, boolean, boolean, EnumMap) 
	 */
	public static Index getInstance( final CharSequence basename, final boolean randomAccess, final boolean documentSizes, final boolean maps ) throws ConfigurationException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		return getInstance( basename, new Properties( basename + DiskBasedIndex.PROPERTIES_EXTENSION ), randomAccess, documentSizes, maps, null );
	}

	
	/** Returns a new disk-based index, guessing reasonable term and prefix maps from the basename.
	 * 
	 * @param basename the basename of the index.
	 * @param randomAccess whether the index should be accessible randomly (e.g., if it will
	 * be possible to call {@link IndexReader#documents(long)} on the index readers returned by the index).
	 * @param documentSizes if true, document sizes will be loaded (note that sometimes document sizes
	 * might be loaded anyway because the compression method for positions requires it).
	 */
	public static Index getInstance( final CharSequence basename, final boolean randomAccess, final boolean documentSizes ) throws ConfigurationException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		return getInstance( basename, randomAccess, documentSizes, true );
	}

	/** Returns a new local index, trying to guess reasonable term and prefix maps from the basename,
	 * and loading document sizes only if it is necessary.
	 * 
	 * @param basename the basename of the index.
	 * @param randomAccess whether the index should be accessible randomly (e.g., if it will
	 * be possible to call {@link IndexReader#documents(long)} on the index readers returned by the index).
	 */
	public static Index getInstance( final CharSequence basename, final boolean randomAccess ) throws ConfigurationException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		return getInstance( basename, randomAccess, false );
	}

	/** Returns a new local index, trying to guess reasonable term and prefix maps from the basename,
	 *  loading offsets but loading document sizes only if it is necessary.
	 * 
	 * @param basename the basename of the index.
	 */
	public static Index getInstance( final CharSequence basename ) throws ConfigurationException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		return getInstance( basename, true );
	}
}
