package it.unimi.di.big.mg4j.tool;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2016 Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.document.DocumentCollectionBuilder;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentFactory.FieldType;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.document.IdentityDocumentFactory;
import it.unimi.di.big.mg4j.document.SimpleCompressedDocumentCollectionBuilder;
import it.unimi.di.big.mg4j.document.SubDocumentFactory;
import it.unimi.di.big.mg4j.index.BitStreamHPIndex;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.BitStreamIndexWriter;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.DowncaseTermProcessor;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.NullTermProcessor;
import it.unimi.di.big.mg4j.index.SkipBitStreamIndexWriter;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.IndexCluster;
import it.unimi.di.big.mg4j.io.IOFactories;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.tool.Combine.IndexType;
import it.unimi.di.big.mg4j.tool.Scan.Completeness;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.dsi.Util;
import it.unimi.dsi.big.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.big.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.big.util.StringMaps;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** An index builder. 
 * 
 * <p>An instance of this class exposes a {@link #run()} method
 * that will index the {@link DocumentSequence} provided at construction time
 * by calling {@link Scan} and {@link Combine} in sequence.
 * 
 * <p>Additionally, a main method provides easy access to index construction.
 * 
 * <p>All indexing parameters are available
 * either as chainable setters that can be called optionally before invoking {@link #run()}, or
 * as public mutable collections and maps. For instance,
 * <pre>
 * new IndexBuilder( "foo", sequence ).skips( true ).run();
 * </pre>
 * will build an index with basename <samp>foo</samp> using skips. If instead we want to 
 * index just the first field of the sequence, and use a {@link ShiftAddXorSignedStringMap}
 * as a term map, we can use the following code:
 * <pre>
 * new IndexBuilder( "foo", sequence )
 *     .termMapClass( ShiftAddXorSignedMinimalPerfectHash.class )
 *     .indexedFields( 0 ).run();
 * </pre>
 * <p>More sophisticated modifications can be applied using public maps:
 * <pre>
 * IndexBuilder indexBuilder = new IndexBuilder( "foo", sequence );
 * indexBuilder.virtualDocumentGaps.put( 0, 30 );
 * indexBuilder.virtualDocumentResolver.put( 0, someVirtualDocumentResolver );
 * indexBuilder.run();
 * </pre>
 *
 */

public class IndexBuilder {
	final static Logger LOGGER = LoggerFactory.getLogger( IndexBuilder.class );

	private final String basename;
	private final DocumentSequence documentSequence;

	private IOFactory ioFactory = IOFactory.FILESYSTEM_FACTORY;

	private TermProcessor termProcessor = DowncaseTermProcessor.getInstance();
	private int documentsPerBatch = Scan.DEFAULT_BATCH_SIZE;
	private int maxTerms = Scan.DEFAULT_MAX_TERMS;
	private boolean keepBatches;

	private Map<Component, Coding> quasiSuccinctWriterFlags = CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX;
	private Map<Component, Coding> standardWriterFlags = CompressionFlags.DEFAULT_STANDARD_INDEX;
	private Map<Component, Coding> payloadWriterFlags = CompressionFlags.DEFAULT_PAYLOAD_INDEX;
	
	private boolean skips = true;
	private IndexType indexType = IndexType.QUASI_SUCCINCT;
	private int quantum = BitStreamIndex.DEFAULT_QUANTUM;
	private int height =BitStreamIndex.DEFAULT_HEIGHT;
	
	private int scanBufferSize = Scan.DEFAULT_BUFFER_SIZE;
	private int combineBufferSize = Combine.DEFAULT_BUFFER_SIZE;
	private int skipBufferSize = SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE;
	private int pasteBufferSize = Paste.DEFAULT_MEMORY_BUFFER_SIZE;

	private String batchDirName;
	/** The set of indexed fields (expressed as field indices). If left empty, <em>all</em> fields will be indexed,
	 * with the proviso that fields of type {@link FieldType#VIRTUAL} will be indexed only
	 * if they have a corresponding {@link VirtualDocumentResolver}.
	 * 
	 * <p>An alternative, chained access to this map is provided by the method {@link #indexedFields(int[])}
	 * 
	 * <p>After calling {@link #run()}, this map will contain the set of fields actually indexed.
	 */
	
	public IntSortedSet indexedFields = new IntRBTreeSet();
	
	/** A map from field indices to a corresponding {@link VirtualDocumentResolver}. */
	public Int2ObjectMap<VirtualDocumentResolver> virtualDocumentResolvers = new Int2ObjectArrayMap<VirtualDocumentResolver>();

	private Class<? extends StringMap<? extends CharSequence>> termMapClass = ImmutableExternalPrefixMap.class;
	
	/** A map from field indices to virtual gaps. Only values associated with fields of type {@link FieldType#VIRTUAL} are meaningful,
	 * and the {@linkplain Object2IntMap#defaultReturnValue() default return value} is set fo {@link Scan#DEFAULT_VIRTUAL_DOCUMENT_GAP}. You
	 * can either add entries, or change the default return value. */
	public Int2IntMap virtualDocumentGaps = new Int2IntArrayMap();
	{
		virtualDocumentGaps.defaultReturnValue( Scan.DEFAULT_VIRTUAL_DOCUMENT_GAP );
	}
	
	private String mapFile;

	private long logInterval = ProgressLogger.DEFAULT_LOG_INTERVAL;

	private DocumentCollectionBuilder builder;

	/** Creates a new index builder with default parameters.
	 * 
	 * <p>Note, in particular, that the resulting index will be a {@linkplain BitStreamHPIndex}
	 * (unless you require payloads, in which case it will be a {@link BitStreamIndex} with skips),
	 * and that all terms will be {@linkplain DowncaseTermProcessor downcased}. You can set
	 * more finely the type of index using {@link #interleaved(boolean)} and {@link #skips(boolean)}.
	 * 
	 * @param basename the basename from which all files will be stemmed.
	 * @param documentSequence the document sequence to be indexed.
	 */
	
	public IndexBuilder( String basename, final DocumentSequence documentSequence ) {
		this.basename = basename;
		this.documentSequence = documentSequence;
	}

	/** Sets the I/O factory (default: {@link IOFactory#FILESYSTEM_FACTORY}).
	 * 
	 * @param ioFactory the I/O factory.
	 * @return this index builder.
	 */
	public IndexBuilder ioFactory( final IOFactory ioFactory ) {
		this.ioFactory = ioFactory;
		return this;
	}
	
	/** Sets the term processor (default: {@link DowncaseTermProcessor}).
	 * 
	 * @param termProcessor the term processor.
	 * @return this index builder.
	 */
	public IndexBuilder termProcessor( final TermProcessor termProcessor ) {
		this.termProcessor = termProcessor;
		return this;
	}

	/** Sets the document collection builder (default: <code>null</code>).
	 * 
	 * @param builder a document-collection builder class that will be used to build a collection during the indexing phase.
	 * @return this index builder.
	 */
	public IndexBuilder builder( final DocumentCollectionBuilder builder ) {
		this.builder = builder;
		return this;
	}

	/** Sets the indexed fields to those provided (default: all fields, but see {@link #indexedFields}).
	 * 
	 * <p>This is a utility method that provides a way to set {@link #indexedFields} in a chainable way.
	 * 
	 * @param field a list of fields to be indexed, that will <em>replace</em> the current values in {@link #indexedFields}.
	 * @return this index builder.
	 * @see IndexBuilder#indexedFields
	 */
	public IndexBuilder indexedFields( int... field ) {
		indexedFields.clear();
		for( int f: field ) indexedFields.add( f );
		return this;
	}

	/** Adds a virtual document resolver to {@link #virtualDocumentResolvers}.
	 * 
	 * <p>This is a utility method that provides a way to put an element into {@link #virtualDocumentResolvers} in a chainable way.
	 * 
	 * @param field a field index.
	 * @param virtualDocumentResolver a virtual document resolver.
	 * @return this index builder.
	 * @see IndexBuilder#virtualDocumentResolvers
	 */
	public IndexBuilder virtualDocumentResolver( final int field, final VirtualDocumentResolver virtualDocumentResolver ) {
		virtualDocumentResolvers.put( field, virtualDocumentResolver );
		return this;
	}

	/** Sets the {@link Scan} buffer size (default: {@link Scan#DEFAULT_BUFFER_SIZE}).
	 * 
	 * @param bufferSize a buffer size for {@link Scan}.
	 * @return this index builder.
	 */
	public IndexBuilder scanBufferSize( final int bufferSize ) {
		this.scanBufferSize = bufferSize;
		return this;
	}
	
	/** Sets the {@link Combine} buffer size (default: {@link Combine#DEFAULT_BUFFER_SIZE}).
	 * 
	 * @param bufferSize a buffer size for {@link Combine}.
	 * @return this index builder.
	 */
	public IndexBuilder combineBufferSize( final int bufferSize ) {
		this.combineBufferSize = bufferSize;
		return this;
	}
	
	/** Sets both the {@linkplain #scanBufferSize(int) scan buffer size} and the {@linkplain #combineBufferSize(int) combine buffer size}.
	 * 
	 * @param bufferSize a buffer size.
	 * @return this index builder.
	 */
	public IndexBuilder bufferSize( final int bufferSize ) {
		scanBufferSize( bufferSize );
		combineBufferSize( bufferSize );
		return this;
	}

	/** Sets the size in byte of the internal buffer using during the construction of a index with skips (default: {@link SkipBitStreamIndexWriter#DEFAULT_TEMP_BUFFER_SIZE}).
	 * 
	 * @param bufferSize a buffer size for {@link SkipBitStreamIndexWriter}.
	 * @return this index builder.
	 */
	public IndexBuilder skipBufferSize( final int bufferSize ) {
		this.skipBufferSize  = bufferSize;
		return this;
	}
	
	/** Sets the size in byte of the internal buffer using when {@linkplain Paste pasting indices} (default: {@link Paste#DEFAULT_MEMORY_BUFFER_SIZE}).
	 * 
	 * @param bufferSize a buffer size for {@link Paste}.
	 * @return this index builder.
	 */
	public IndexBuilder pasteBufferSize( final int bufferSize ) {
		this.pasteBufferSize  = bufferSize;
		return this;
	}
		
	/** Sets the number of documents per batch (default: {@link Scan#DEFAULT_BATCH_SIZE}).
	 * 
	 * @param documentsPerBatch the number of documents {@link Scan} will attempt to add to each batch.
	 * @return this index builder.
	 */
	public IndexBuilder documentsPerBatch( final int documentsPerBatch ) {
		this.documentsPerBatch = documentsPerBatch;
		return this;
	}
	
	/** Sets the maximum number of overall (i.e., cross-field) terms per batch (default: {@link Scan#DEFAULT_BATCH_SIZE}).
	 * 
	 * @param maxTerms the maximum number of overall (i.e., cross-field) terms {@link Scan} will attempt to add to each batch.
	 * @return this index builder.
	 */
	public IndexBuilder maxTerms( final int maxTerms ) {
		this.maxTerms = maxTerms;
		return this;
	}
	
	/** Sets the &ldquo;keep batches&rdquo; flag (default: false). If true, the temporary batch files generated
	 * during index construction wil not be deleted.
	 * 
	 * @param keepBatches the new value for the &ldquo;keep batches&rdquo; flag.
	 * @return this index builder.
	 */
	public IndexBuilder keepBatches( final boolean keepBatches ) {
		this.keepBatches = keepBatches;
		return this;
	}
	
	/** Sets the writer compression flags for standard indices (default: {@link CompressionFlags#DEFAULT_STANDARD_INDEX}).
	 * 
	 * @param standardWriterFlags the flags for standard indices.
	 * @return this index builder.
	 */
	public IndexBuilder standardWriterFlags( final Map<Component,Coding> standardWriterFlags ) {
		this.standardWriterFlags = standardWriterFlags;
		return this;
	}
	
	/** Sets the writer compression flags for standard indices (default: {@link CompressionFlags#DEFAULT_QUASI_SUCCINCT_INDEX}).
	 * 
	 * @param quasiSuccinctWriterFlags the flags for quasi-succinct indices.
	 * @return this index builder.
	 */
	public IndexBuilder quasiSuccinctWriterFlags( final Map<Component,Coding> quasiSuccinctWriterFlags ) {
		this.quasiSuccinctWriterFlags = quasiSuccinctWriterFlags;
		return this;
	}
	
	/** Sets the writer compression flags for payload-based indices (default: {@link CompressionFlags#DEFAULT_PAYLOAD_INDEX}).
	 * 
	 * @param payloadWriterFlags the flags for payload-based indices.
	 * @return this index builder.
	 */
	public IndexBuilder payloadWriterFlags( final Map<Component,Coding> payloadWriterFlags ) {
		this.payloadWriterFlags = payloadWriterFlags;
		return this;
	}
	
	/** Sets the skip flag (default: true). If true, the index will have a skipping structure. The
	 * flag is a no-op unless you require an {@linkplain #interleaved(boolean) interleaved index}, as high-performance indices always have skips.
	 * 
	 * @param skips the new value for the skip flag.
	 * @return this index builder.
	 */
	public IndexBuilder skips( final boolean skips ) {
		this.skips = skips;
		return this;
	}
	
	/** Sets the interleaved flag (default: false). If true, the index will be forced to be an {@linkplain BitStreamIndexWriter interleaved index} (but
	 * note that in a number of cases, such as missing index components or payloads, the index will be necessarily interleaved).
	 * 
	 * @param interleaved the new value for the interleaved flag.
	 * @return this index builder.
	 */
	public IndexBuilder interleaved( final boolean interleaved ) {
		this.indexType = Combine.IndexType.INTERLEAVED;
		return this;
	}
	
	/** Sets the {@linkplain Combine.IndexType type} of the index to be built (default: {@link IndexType#QUASI_SUCCINCT}).
	 * 
	 * @param indexType the desired index type.
	 * @return this index builder.
	 */
	public IndexBuilder indexType( final IndexType indexType ) {
		this.indexType = indexType;
		return this;
	}
	
	/** Sets the skip quantum (default: {@link BitStreamIndex#DEFAULT_QUANTUM}).
	 * 
	 * @param quantum the skip quantum.
	 * @return this index builder.
	 */
	public IndexBuilder quantum( final int quantum ) {
		this.quantum = quantum;
		return this;
	}
	
	/** Sets the skip height (default: {@link BitStreamIndex#DEFAULT_HEIGHT}).
	 * 
	 * @param height the skip height.
	 * @return this index builder.
	 */
	public IndexBuilder height( final int height ) {
		this.height = height;
		return this;
	}
	
	/** Sets the name of a file containing a map on the document indices (default: <code>null</code>).
	 * 
	 * <p>The provided file must containing integers in {@link DataOutput} format. They must by as
	 * many as the number of documents in the collection provided at construction time, and the
	 * resulting function must be injective (i.e., there must be no duplicates). 
	 * 
	 * @param mapFile a file representing a document map (or <code>null</code> for no mapping).
	 * @return this index builder.
	 */
	public IndexBuilder mapFile( final String mapFile ) {
		this.mapFile = mapFile;
		return this;
	}

	/** Sets the logging time interval (default: {@link ProgressLogger#DEFAULT_LOG_INTERVAL}).
	 * 
	 * @param logInterval the logging time interval.
	 * @return this index builder.
	 */
	public IndexBuilder logInterval( final long logInterval ) {
		this.logInterval = logInterval;
		return this;
	}
	
	/** Sets the temporary directory for batches (default: the directory containing the basename).
	 * 
	 * @param batchDirName the name of the temporary directory for batches, or <code>null</code> for the directory containing the basename.
	 * @return this index builder.
	 */
	public IndexBuilder batchDirName( final String batchDirName ) {
		this.batchDirName = batchDirName;
		return this;
	}
		
	/** Sets the class used to build the index term map (default: {@link ImmutableExternalPrefixMap}).
	 * 
	 * <p>The only requirement for <code>termMapClass</code> (besides, of course, implementing {@link StringMap})
	 * is that of having a public constructor accepting a single parameter of type <samp>{@link Iterable}&lt;{@link CharSequence}></samp>.
	 * 
	 * @param termMapClass the class used to build the index term map, or <code>null</code> to disable the construction of a term map.
	 * @return this index builder.
	 */
	public IndexBuilder termMapClass( final Class<? extends StringMap<? extends CharSequence>> termMapClass ) {
		if ( ( this.termMapClass = termMapClass ) != null )
			try {
				termMapClass.getConstructor( Iterable.class );
			}
			catch ( Exception e ) {
				throw new IllegalArgumentException( "Class " + termMapClass + " have no constructor accepting an Iterable" );
			}
		return this;
	}

	/** Builds the index.
	 * 
	 * <p>This method simply invokes {@link Scan} and {@link Combine} using the internally stored settings, and
	 * finally builds a {@link StringMap}.
	 * 
	 * <p>If the provided document sequence can be iterated over several times, this method can be called several
	 * times, too, rebuilding each time the index. 
	 */
	public void run() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		final DocumentFactory factory = documentSequence.factory();
		if ( indexedFields.isEmpty() ) {
			// We index everything
			for( int i = 0; i < factory.numberOfFields(); i++ ) 
				if ( factory.fieldType( i ) != FieldType.VIRTUAL || virtualDocumentResolvers.containsKey( i ) ) indexedFields.add( i );
		}
		
		final int[] indexedField = indexedFields.toIntArray();
		final String[] basenameField = new String[ indexedField.length ];
		for( int i = 0; i < indexedField.length; i++ ) basenameField[ i ] = basename + "-" + factory.fieldName( indexedField[ i ] );
		LOGGER.info( "Creating indices " + Arrays.toString( basenameField ) + "..." );

		// Create gap array
		final int[] virtualDocumentGap = new int[ indexedField.length ];
		for( int i = 0; i < indexedField.length; i++ ) virtualDocumentGap[ i ] = virtualDocumentGaps.get( i ); 
		
		// Create virtual document resolver array
		final VirtualDocumentResolver[] virtualDocumentResolver = new VirtualDocumentResolver[ indexedField.length ];
		for( int i: virtualDocumentResolvers.keySet() ) virtualDocumentResolver[ i ] = virtualDocumentResolvers.get( i ); 
				
		Map<Component, Coding> flags = indexType == IndexType.QUASI_SUCCINCT ? quasiSuccinctWriterFlags : standardWriterFlags;
		final Completeness completeness = flags.containsKey( Component.POSITIONS ) ? Scan.Completeness.POSITIONS :
			flags.containsKey( Component.COUNTS ) ? Scan.Completeness.COUNTS :
				Scan.Completeness.POINTERS;

		Scan.run( ioFactory,
				basename, 
				documentSequence, 
				completeness,
				termProcessor,
				builder,
				scanBufferSize,
				documentsPerBatch,
				maxTerms,
				indexedField,
				virtualDocumentResolver,
				virtualDocumentGap,
				mapFile,
				logInterval,
				batchDirName);

		if ( virtualDocumentResolver != null ) Arrays.fill( virtualDocumentResolver, null ); // Let's keep the garbage collector happy
		
		final File batchDir = batchDirName == null ? null : new File( batchDirName );

		for ( int i = 0; i < indexedField.length; i++ ) {
			final int batches;
			if ( factory.fieldType( indexedField[ i ] ) == DocumentFactory.FieldType.VIRTUAL ) {
				batches = IOFactories.loadProperties( ioFactory, basenameField[ i ] + DiskBasedIndex.PROPERTIES_EXTENSION ).getInt( Index.PropertyKeys.BATCHES );
				final String[] inputBasename = new String[ batches ];
				for( int j = 0; j < inputBasename.length; j++ ) inputBasename[ j ] = Scan.batchBasename( j, basenameField[ i ], batchDir ); 
				new Paste( ioFactory, basenameField[ i ], inputBasename, false, false, combineBufferSize, batchDir, pasteBufferSize, flags, indexType, skips, quantum, height, skipBufferSize, logInterval ).run();
			}
			else {
				final String[] inputBasename = IOFactories.loadProperties( ioFactory, basenameField[ i ] + Scan.CLUSTER_PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
				//final String[] inputBasename = new Properties( new File( batchDir, basenameField[ i ] ) + Scan.CLUSTER_PROPERTIES_EXTENSION ).getStringArray( IndexCluster.PropertyKeys.LOCALINDEX );
				batches = inputBasename.length;
				if ( factory.fieldType( indexedField[ i ] ) == DocumentFactory.FieldType.TEXT ) {
					if ( mapFile != null ) new Merge( ioFactory, basenameField[ i ], inputBasename, false, combineBufferSize, flags, indexType, skips, quantum, height, skipBufferSize, logInterval ).run();
					else new Concatenate( ioFactory, basenameField[ i ], inputBasename, false, combineBufferSize, flags, indexType, skips, quantum, height, skipBufferSize, logInterval ).run();
				}
				else {
					if ( mapFile != null ) new Merge( ioFactory, basenameField[ i ], inputBasename, false, combineBufferSize, payloadWriterFlags, IndexType.INTERLEAVED, skips, quantum, height, skipBufferSize, logInterval ).run();
					else new Concatenate( ioFactory, basenameField[ i ], inputBasename, false, combineBufferSize, payloadWriterFlags, IndexType.INTERLEAVED, skips, quantum, height, skipBufferSize, logInterval ).run();
				} 
			}

			// TODO: this is a bit dirty, because in the else above we actually use the batch names found in the cluster property files.
			if ( ! keepBatches ) Scan.cleanup( ioFactory, basenameField[ i ], batches, batchDir );
		}
		
		if ( termMapClass != null ) {
			LOGGER.info( "Creating term maps (class: " + termMapClass.getSimpleName() + ")..." );
			for( int i = 0; i < indexedField.length; i++ ) 
				IOFactories.storeObject( ioFactory, StringMaps.synchronize( termMapClass.getConstructor( Iterable.class ).newInstance( IOFactories.fileLinesCollection( ioFactory, basenameField[ i ] + DiskBasedIndex.TERMS_EXTENSION, "UTF-8" ) ) ), basenameField[ i ] + DiskBasedIndex.TERMMAP_EXTENSION  );
		}

		LOGGER.info( "Indexing completed." );
	}
	
	@SuppressWarnings({ "unchecked", "resource" })
	public static void main( final String[] arg ) throws JSAPException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, ConfigurationException, ClassNotFoundException, IOException, InstantiationException, URISyntaxException {
	
		SimpleJSAP jsap = new SimpleJSAP( IndexBuilder.class.getName(), "Builds an index (creates batches, combines them, and builds a term map).",
				new Parameter[] {
				new FlaggedOption( "sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "sequence", "A serialised document sequence that will be used instead of stdin." ),
				new FlaggedOption( "ioFactory", JSAP.STRING_PARSER, "FILESYSTEM_FACTORY", JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "io-factory", "An I/O factory that will be used to create files (either a static field of IOFactory or an object specification)." ),
				new FlaggedOption( "objectSequence", new ObjectParser( DocumentSequence.class, MG4JClassParser.PACKAGE ), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "object-sequence", "An object specification describing a document sequence that will be used instead of stdin." ),
				new FlaggedOption( "delimiter", JSAP.INTEGER_PARSER, Integer.toString( Scan.DEFAULT_DELIMITER ), JSAP.NOT_REQUIRED, 'd', "delimiter", "The document delimiter (when indexing stdin)." ),
				new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor (when indexing stdin)." ),
				new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file (when indexing stdin)." ).setAllowMultipleDeclarations( true ),
				new FlaggedOption( "termProcessor", JSAP.STRING_PARSER, NullTermProcessor.class.getName(), JSAP.NOT_REQUIRED, 't', "term-processor", "Sets the term processor to the given class." ),
				new FlaggedOption( "termMap", MG4JClassParser.getParser(), ImmutableExternalPrefixMap.class.getName(), JSAP.NOT_REQUIRED, 'm', "term-map", "Sets the term map class." ),
				new Switch( "downcase", JSAP.NO_SHORTFLAG, "downcase", "A shortcut for setting the term processor to the downcasing processor." ),
				new FlaggedOption( "indexedField", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'I', "indexed-field", "The field(s) of the document factory that will be indexed. (default: all fields)" ).setAllowMultipleDeclarations( true ),
				new Switch( "allFields", 'a', "all-fields", "Index also all virtual fields; has no effect if indexedField has been used at least once." ),
				new FlaggedOption( "batchSize", JSAP.INTSIZE_PARSER, Integer.toString( Scan.DEFAULT_BATCH_SIZE ), JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in documents. Batches will be smaller, however, if memory is exhausted." ),
				new FlaggedOption( "maxTerms", JSAP.INTSIZE_PARSER, Integer.toString( Scan.DEFAULT_MAX_TERMS ), JSAP.NOT_REQUIRED, 'M', "max-terms", "The maximum number of terms in a batch, in documents." ),
				new Switch( "keepBatches", JSAP.NO_SHORTFLAG, "keep-batches", "Do not delete intermediate batch files." ),
				new FlaggedOption( "virtualDocumentResolver", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'v', "virtual-document-resolver", "The virtual document resolver. It can be specified several times in the form [<field>:]<filename>. If the field is omitted, it sets the document resolver for all virtual fields." ).setAllowMultipleDeclarations( true ),
				new FlaggedOption( "virtualDocumentGap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'g', "virtual-document-gap", "The virtual document gap. It can be specified several times in the form [<field>:]<gap>. If the field is omitted, it sets the document gap for all virtual fields; the default gap is " + Scan.DEFAULT_VIRTUAL_DOCUMENT_GAP ).setAllowMultipleDeclarations( true ),
				new FlaggedOption( "scanBufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( Scan.DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, 'b', "scan-buffer-size", "The size of an I/O buffer for the scanning phase." ),
				new FlaggedOption( "combineBufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( Combine.DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "combine-buffer-size", "The size of an I/O buffer for the combination phase." ),
				new FlaggedOption( "pasteBufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( Paste.DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "paste-buffer-size", "The size of the internal temporary buffer used while pasting indices." ),
				new FlaggedOption( "skipBufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE ), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "skip-buffer-size", "The size of the internal temporary buffer used while creating an index with skips." ),
				new FlaggedOption( "renumber", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "renumber", "The filename of a document renumbering." ),
				new FlaggedOption( "zipCollection", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'z', "zip", "Creates a support ZipDocumentCollection with given basename (obsolete)." ),
				new FlaggedOption( "buildCollection", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'B', "build-collection", "During the indexing phase, build a collection using this basename." ),
				new FlaggedOption( "builderClass", MG4JClassParser.getParser(), SimpleCompressedDocumentCollectionBuilder.class.getName(), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "builder-class", "Specifies a builder class for a document collection that will be created during the indexing phase." ),
				new Switch( "exact", 'e', "exact", "The builder class should be instantiated in its exact form, which records both words and nonwords." ),
				new FlaggedOption( "comp", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "comp", "A compression flag for textual indices (may be specified several times)." ).setAllowMultipleDeclarations( true ),
				new FlaggedOption( "payloadComp", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'C', "comp-payload", "A compression flag for payload indices (may be specified several times)." ).setAllowMultipleDeclarations( true ),
				new Switch( "noSkips", JSAP.NO_SHORTFLAG, "no-skips", "Disables skips." ),
				new Switch( "interleaved", JSAP.NO_SHORTFLAG, "interleaved", "Forces an interleaved index." ),
				new Switch( "highPerformance", 'h', "high-performance", "Forces a high-performance index." ),
				new FlaggedOption( "quantum", JSAP.INTEGER_PARSER, Integer.toString( BitStreamIndex.DEFAULT_QUANTUM ), JSAP.NOT_REQUIRED, 'Q', "quantum", "For quasi-succinct indices, the size of the quantum (-1 implies the default quantum). For other indices, enable skips with given quantum, if positive; fix space occupancy of variable-quantum skip towers in percentage if negative." ),
				new FlaggedOption( "height", JSAP.INTSIZE_PARSER, Integer.toString( BitStreamIndex.DEFAULT_HEIGHT ), JSAP.NOT_REQUIRED, 'H', "height", "Enable skips with given height." ),
				new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
				new FlaggedOption( "tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "temp-dir", "A directory for all temporary batch files." ),
				new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the resulting index." )
		});

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;

		if ( ( jsapResult.userSpecified( "builderClass" ) || jsapResult.userSpecified( "exact" ) ) && ! jsapResult.userSpecified( "buildCollection" ) )	throw new IllegalArgumentException( "To specify options about the collection building process, you must specify a basename first." );
		if ( jsapResult.userSpecified( "sequence" ) && jsapResult.userSpecified( "objectSequence" ) ) throw new IllegalArgumentException( "You cannot specify both a serialised and an parseable-object sequence" );

		final IOFactory ioFactory = Scan.parseIOFactory( jsapResult.getString( "ioFactory" ) );
		
		final DocumentSequence documentSequence = jsapResult.userSpecified( "objectSequence" ) ? (DocumentSequence)jsapResult.getObject( "objectSequence" ) : Scan.getSequence( jsapResult.getString( "sequence" ), jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ), jsapResult.getInt( "delimiter" ), LOGGER );
		final DocumentFactory factory = documentSequence.factory();

		final int[] indexedField = Scan.parseFieldNames( jsapResult.getStringArray( "indexedField" ), factory, jsapResult.getBoolean( "allFields" ) );
		final VirtualDocumentResolver[] virtualDocumentResolver = Scan.parseVirtualDocumentResolver( ioFactory, jsapResult.getStringArray( "virtualDocumentResolver" ), indexedField, factory );
		final int[] virtualDocumentGap = Scan.parseVirtualDocumentGap( jsapResult.getStringArray( "virtualDocumentGap" ), indexedField, factory );

		final TermProcessor termProcessor = jsapResult.getBoolean( "downcase" ) ? DowncaseTermProcessor.getInstance() :
			ObjectParser.fromSpec( jsapResult.getString( "termProcessor" ), TermProcessor.class, MG4JClassParser.PACKAGE, new String[] { "getInstance" } ); 

		final boolean skips = ! jsapResult.getBoolean( "noSkips" );
		final boolean interleaved = jsapResult.getBoolean( "interleaved" );
		final boolean highPerformance = jsapResult.getBoolean( "highPerformance" );
		if ( interleaved && highPerformance ) throw new IllegalArgumentException( "You must specify either --interleaved or --high-performance." );
		if ( ! skips && ! interleaved ) throw new IllegalArgumentException( "You can disable skips only for interleaved indices" );
		if ( ! skips && ( jsapResult.userSpecified( "quantum" ) || jsapResult.userSpecified( "height" ) ) ) throw new IllegalArgumentException( "You specified quantum or height, but you also disabled skips." );

		DocumentCollectionBuilder builder = null;
		if ( jsapResult.userSpecified( "buildCollection" ) ) {
			final Class<? extends DocumentCollectionBuilder> builderClass = jsapResult.getClass( "builderClass" );
			try {
				// Try first IOFactory-based constructor.
				builder = builderClass != null ? builderClass.getConstructor( IOFactory.class, String.class, DocumentFactory.class, boolean.class ).newInstance( 
						ioFactory, jsapResult.getString( "buildCollection" ), 
						documentSequence.factory().numberOfFields() == indexedField.length ? documentSequence.factory().copy() : new SubDocumentFactory( documentSequence.factory().copy(), indexedField ), 
						Boolean.valueOf( jsapResult.getBoolean( "exact" ) ) ) : null;
			}
			catch( NoSuchMethodException noIOFactoryConstructor ) {
				builder = builderClass != null ? builderClass.getConstructor( String.class, DocumentFactory.class, boolean.class ).newInstance( 
						jsapResult.getString( "buildCollection" ), 
						documentSequence.factory().numberOfFields() == indexedField.length ? documentSequence.factory().copy() : new SubDocumentFactory( documentSequence.factory().copy(), indexedField ), 
						Boolean.valueOf( jsapResult.getBoolean( "exact" ) ) ) : null;
				if ( builder != null ) LOGGER.warn( "The builder class " + builderClass.getName() + " has no IOFactory-based constructor" );
			}
		}

		final IndexBuilder indexBuilder = new IndexBuilder( jsapResult.getString( "basename" ), documentSequence )
		.ioFactory( ioFactory )
		.termProcessor( termProcessor )
		.builder( builder )
		.scanBufferSize( jsapResult.getInt( "scanBufferSize" ) )
		.skipBufferSize( jsapResult.getInt( "skipBufferSize" ) )
		.pasteBufferSize( jsapResult.getInt( "pasteBufferSize" ) )
		.combineBufferSize( jsapResult.getInt( "combineBufferSize" ) )
		.documentsPerBatch( jsapResult.getInt( "batchSize" ) )
		.maxTerms( jsapResult.getInt( "maxTerms" ) )
		.keepBatches( jsapResult.getBoolean( "keepBatches" ) )
		.termMapClass( jsapResult.getClass( "termMap" ) )
		.indexedFields( indexedField )
		.skips( skips )
		.indexType( interleaved ? IndexType.INTERLEAVED : highPerformance ? IndexType.HIGH_PERFORMANCE : IndexType.QUASI_SUCCINCT )
		.quantum( jsapResult.getInt( "quantum" ) )
		.height( jsapResult.getInt( "height" ) )
		.logInterval( jsapResult.getLong( "logInterval" ) )
		.batchDirName( jsapResult.getString( "tempDir" ) );
		
		for( int i = 0; i < virtualDocumentResolver.length; i++ ) if ( virtualDocumentResolver[ i ] != null ) indexBuilder.virtualDocumentResolvers.put( i, virtualDocumentResolver[ i ] );
		for( int i = 0; i < virtualDocumentGap.length; i++ ) indexBuilder.virtualDocumentGaps.put( i, virtualDocumentGap[ i ] );
		
		if ( jsapResult.userSpecified( "comp" ) ) {
			if ( interleaved || highPerformance ) indexBuilder.standardWriterFlags( CompressionFlags.valueOf( jsapResult.getStringArray( "comp" ), CompressionFlags.DEFAULT_STANDARD_INDEX ) );
			else indexBuilder.quasiSuccinctWriterFlags( CompressionFlags.valueOf( jsapResult.getStringArray( "comp" ), CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX ) );
		}
		if ( jsapResult.userSpecified( "compPayload" ) ) indexBuilder.payloadWriterFlags( CompressionFlags.valueOf( jsapResult.getStringArray( "compPayload" ), CompressionFlags.DEFAULT_PAYLOAD_INDEX ) );
		if ( jsapResult.userSpecified( "renumber" ) ) indexBuilder.mapFile( jsapResult.getString( "renumber" ) );
		
		indexBuilder.run();
	}
}
