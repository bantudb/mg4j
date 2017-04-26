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

import it.unimi.di.big.mg4j.document.AbstractDocumentSequence;
import it.unimi.di.big.mg4j.document.ConcatenatedDocumentCollection;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentCollectionBuilder;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.document.IdentityDocumentFactory;
import it.unimi.di.big.mg4j.document.InputStreamDocumentSequence;
import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.di.big.mg4j.document.SimpleCompressedDocumentCollectionBuilder;
import it.unimi.di.big.mg4j.document.SubDocumentFactory;
import it.unimi.di.big.mg4j.index.BitStreamIndexWriter;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.DowncaseTermProcessor;
import it.unimi.di.big.mg4j.index.FileIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexWriter;
import it.unimi.di.big.mg4j.index.NullTermProcessor;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.ContiguousDocumentalStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalCluster;
import it.unimi.di.big.mg4j.index.cluster.DocumentalConcatenatedCluster;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.di.big.mg4j.index.cluster.IdentityDocumentalStrategy;
import it.unimi.di.big.mg4j.index.cluster.IndexCluster;
import it.unimi.di.big.mg4j.index.payload.DatePayload;
import it.unimi.di.big.mg4j.index.payload.IntegerPayload;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.ByteArrayPostingList;
import it.unimi.di.big.mg4j.io.IOFactories;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.util.MG4JClassParser;
import it.unimi.di.big.mg4j.util.parser.callback.AnchorExtractor;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntBigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.longs.AbstractLongComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Properties;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;
import com.martiansoftware.jsap.stringparsers.LongSizeStringParser;


/**
 * Scans a document sequence, dividing it in batches of occurrences and writing for each batch a
 * corresponding subindex.
 * 
 * <P>This class (more precisely, its
 * {@link #run(String, DocumentSequence, Completeness, TermProcessor, DocumentCollectionBuilder, int, int, int, int[], VirtualDocumentResolver[], int[], String, long, String) run()}
 * method) reads a document sequence and produces several <em>batches</em>, that is, subindices
 * corresponding to subsets of term/document pairs of the collection. A set of batches is generated
 * for each indexed field of the collection. A main method invokes the above method setting its
 * parameters using suitable options. Usually, batches are then merged into the actual index (as
 * it happens with {@link IndexBuilder}).
 * 
 * <p>Unless a serialised {@link it.unimi.di.big.mg4j.document.DocumentSequence} is specified using
 * the suitable option, an implicit {@link it.unimi.di.big.mg4j.document.InputStreamDocumentSequence}
 * is created using separator byte (default is 10, i.e., newline). In the latter case, the factory
 * and its properties can be set with command-line options.
 * 
 * <P>The only mandatory argument is a <em>basename</em>, which will be used to stem the names
 * of all files generated. The first batch of a field named <var>field</var> will use the basename
 * <code><var>basename-field</var>@0</code>, the second batch <code><var>basename-field</var>@1</code>
 * and so on. It is also possible to specify a separate directory for batch files (e.g., for easier
 * {@linkplain #cleanup(IOFactory, String, int, File) cleanup} when they are no longer necessary).
 * 
 * <P>Since documents are read sequentially, every document has a <em>natural
 * index</em> starting
 * from 0. If no remapping (i.e., renumbering) is specified, the <em>document
 * index</em> of each document
 * corresponds to its natural index. If, however, a remapping is specified, under the form of a
 * list of integers, the document index of a document is the integer found in the corresponding
 * position of the list. More precisely, a remapping for <var>N</var> documents is a list of
 * <var>N</var> distinct integers, and a document with natural index <var>i</var> has document
 * index given by the <var>i</var>-th element of the list. This is useful when indexing statically
 * ranked documents (e.g., if you are indexing a part of the web and would like the index to return
 * documents with higher static rank first). If the remapping file is provided, it must be a
 * sequence of integers in {@link DataInput} format; if
 * <var>N</var> is the number of documents, the file is to contain exactly <var>N</var> distinct
 * integers. The integers need not be between 0 and <var>N</var>-1, to allow the remapping of
 * subindices (but a warning will be logged in this case, just to be sure you know what you're doing).
 * 
 * <P>Also every term has an associated number starting from 0, assigned in lexicographic order.
 * 
 * <h2>Index types and indexing types</h2>
 * 
 * <p>A <em>standard index</em> contains a list of terms, and for each term a posting list. Each
 * posting contains mandatorily a document pointer, and then, optionally, the count and the
 * positions of the term (whether the last two elements appear can be specified using suitable
 * {@linkplain CompressionFlags compression flags}).
 * 
 * <p>The indexing type of a standard index can be {@link IndexingType#STANDARD},
 * {@link IndexingType#REMAPPED} or {@link IndexingType#VIRTUAL}. In the first case, we index the
 * words occurring in documents as usual. In the second case, before writing the index all documents
 * are renumbered following a provided map. In the third case (used only with
 * {@link it.unimi.di.big.mg4j.document.DocumentFactory.FieldType#VIRTUAL} fields) indexing is performed on a virtual document
 * obtained by collating a number of {@linkplain VirtualDocumentFragment fragments}. 
 * Fragments are associated with documents by some key,
 * and a {@link VirtualDocumentResolver} turns a key into a document natural number, so that the
 * collation process can take place (a settable gap is inserted between fragments).
 * 
 * <p>Besides storing document pointers, document counts, and position, MG4J makes it possible to
 * store an arbitrary <em>payload</em> with each posting. This feature is presently used only to
 * create <em>payload-based indices</em>&mdash;indices without counts and positions that contain
 * a single, dummy word <code>#</code>. They are actually used to store arbitrary data associated
 * to each document, such as dates and integers: using a special syntax, is then possible to specify
 * <em>range queries</em> on the values of such fields.
 * 
 * <p>The main difference between standard and payload-based indices is that the first type is
 * handled by instances of this class, whereas the second type is handled by instances of
 * {@link Scan.PayloadAccumulator}. The
 * {@link #run(String, DocumentSequence, Completeness, TermProcessor, DocumentCollectionBuilder, int, int, int, int[], VirtualDocumentResolver[], int[], String, long, String) run()}
 * method creates a set of suitable instances, one for each indexed field, and feeds them in
 * parallel with data from the appropriate field of the same document.
 *
 * <p>Note that this class uses an internal hack that mimicks {@link BitStreamIndexWriter} 
 * to perform a lightweight in-memory inversion that generates directly compressed posting lists.
 * As a consequence, codes are fixed (see {@link CompressionFlags#DEFAULT_STANDARD_INDEX} and {@link CompressionFlags#DEFAULT_PAYLOAD_INDEX}).
 * The only choice you have is the <em>completeness</em> of the index, which can range
 * from {@linkplain Completeness#POINTERS pointers} to {@linkplain Completeness#COUNTS counts} 
 * up to full {@linkplain Completeness#POSITIONS positions}. More sophisticated choices (e.g., coding,
 * skipping structures, etc.) can be obtained when {@linkplain Combine combining} the batches.
 *
 * <h2>Building collections while indexing</h2>
 * 
 * <p>During the indexing process, a {@link DocumentCollectionBuilder} can be used to generate a document collection
 * that copies the sequence used to generate the index. While any builder can be passed to 
 * {@link #run(String, DocumentSequence, Completeness, TermProcessor, DocumentCollectionBuilder, int, int, int, int[], VirtualDocumentResolver[], int[], String, long, String) run()}
 * method, specifying a builder <em>class</em> on the command line requires that the class provides
 * a constructor accepting a basename for the generated collection ({@link CharSequence}), the original
 * factory ({@link DocumentFactory}) and a boolean that specifies whether the collection built should be exact
 * (i.e., if it should index nonwords).
 * 
 * <p>A collection will be generated for each batch (the basename will be the same), so each batch
 * can be used separately as an index with its associated collection. Finally, a {@link ConcatenatedDocumentCollection}
 * will be used to concatenate virtually the collections associated with batches, thus providing
 * a global collection.
 *  
 *  
 * <h2>Batch subdivision and content</h2>
 * 
 * <p>The scanning process will try to
 * build batches containing exactly the specified number of documents per batch
 * (for all indexed fields). There are of
 * course space constraints that could make building exact batches impossible, as the entire data of
 * a batch must into core memory. If memory is too low, a batch will be generated with fewer
 * documents than expected. There is also a maximum number of terms allowed, as a very large number of
 * terms (more than few millions) can cause massive garbage collection: in that case, it is better to dump
 * a batch and just start a new one.
 * 
 * <p>The larger the number of documents in a batch is, the quicker index construction will be.
 * Usually, some experiments and a look at the logs is all that suffices to find out good parameters
 * for the Java virtual machine maximum memory setting the number of documents per batch and the
 * maximum number of terms (these depends on the structure of the collection you are indexing).
 * 
 * <P>Each batch is an {@linkplain BitStreamIndexWriter interleaved index}. Using a suitable option, you can get for each batch an additional  
 * file with extension <code>.terms.unsorted</code> containing the list of indexed terms in the same order in which they were met in the document
 * collection. 
 * 
 * <p>Finally, a file with extension <code>.cluster.properties</code> contains
 * contains information about the set of batches
 * seen as a {@link it.unimi.di.big.mg4j.index.cluster.DocumentalCluster}. Besides the {@linkplain it.unimi.di.big.mg4j.index.Index.PropertyKeys standard keys},
 * the file contains {@link it.unimi.di.big.mg4j.index.cluster.IndexCluster.PropertyKeys#LOCALINDEX} entries specifing the basename
 * of the batches, and a {@linkplain it.unimi.di.big.mg4j.index.cluster.IndexCluster.PropertyKeys#STRATEGY strategy}. After creating manually suitable term maps for
 * each batch, you will be able to access the set of batches as a single index (note, however, that
 * standard batches are very compact but provide <em>slow access</em>).
 * 
 * @author Sebastiano Vigna
 * @since 1.0
 */

public class Scan {
	private final static Logger LOGGER = LoggerFactory.getLogger( Scan.class );
	private final static boolean ASSERTS = false;

	public static enum IndexingType {
		/** A standard index&mdash;documents will be provided in increasing order. */
		STANDARD,
		/** A remapped index&mdash;documents will be provided in separate calls, but in any order. */
		REMAPPED,
		/**
		 * A virtual index&mdash;documents will be provided in any order, and a document may appear
		 * several times.
		 */
		VIRTUAL
	}

	/** An interface that describes a virtual document fragment.
	 *
	 * When indexing in {@link IndexingType#VIRTUAL} mode, documents are composed
	 * by fragments (typically, some text surrounding an anchor) that are referred
	 * to a document by some spefication (in the previous case, the content <code>href</code> attribute
	 * of the anchor). This interface is used to describe such fragments (see, e.g.,
	 * {@link AnchorExtractor}).
	 * 
	 * @see VirtualDocumentResolver
	 */
	
	public static interface VirtualDocumentFragment extends Serializable {
		/** The specification of the document to which this fragment belong.
		 * 
		 * @return the specification of the document to which this fragment belong.
		 * @see VirtualDocumentResolver
		 */
		public MutableString documentSpecifier();
		/** The textual content of this fragment.
		 * 
		 * @return the textual content of this fragment.
		 */
		public MutableString text();
	}
	
	public static enum Completeness {	
		/** Basic completeness level. Just pointers (usable only for Boolean queries). */
		POINTERS,
		/** An index without positions (e.g., for count-based ranking). */
		COUNTS,
		/** A complete index. */
		POSITIONS
	};
	
	/** When available memory goes below this threshold, we try a compaction. */
	public static final int PERC_AVAILABLE_MEMORY_CHECK = 10;
	
	/** If after compaction there is less memory (in percentage) than this value, we will flush the current batch. */
	public static final int PERC_AVAILABLE_MEMORY_DUMP = 30;

	/** The extension of the property file for the cluster associated with a scan. */
	private static final String CLUSTER_STRATEGY_EXTENSION = ".cluster.strategy";

	/** The extension of the strategy for the cluster associated with a scan. */
	public static final String CLUSTER_PROPERTIES_EXTENSION = ".cluster.properties";

	/** The initial size in bytes of a byte array posting list. Most terms have a very low frequency, so we keep it small. */
	private static final int BYTE_ARRAY_POSTING_LIST_INITIAL_SIZE = 8;

	/** The frequency with which we report the current number of terms. */
	private static final int TERM_REPORT_STEP = 1000000;

	/** The initial size of the term map. */
	public static final int INITIAL_TERM_MAP_SIZE = 1000;

	/** The I/O factory that will be used to create files. */
	private final IOFactory ioFactory;

	/** The current basename of the overall index (usually some basename postfixed with the field name). */
	private final String basename;

	/** A term processor to be applied during the indexing phase. */
	private final TermProcessor termProcessor;

	/** The field name, if available. */
	private final String field;

	/** The size of a buffer. */
	private final int bufferSize;

	/** The directory where batches files will be created. */
	private final File batchDir;

	/** The flag map for batches. */
	final Map<Component, Coding> flags;

	/** A map containing the terms seen so far. */
	private Object2ReferenceOpenHashMap<MutableString, ByteArrayPostingList> termMap;

	/**
	 * The output bit stream for size information. For {@link IndexingType#STANDARD} indexing, the
	 * list of &gamma;-coded document sizes. For {@link IndexingType#REMAPPED} indexing, a list of
	 * &gamma;-coded document numbers and document sizes.
	 */
	private OutputBitStream sizes;

	/** The total number of occurrences. */
	private long totOccurrences;

	/** The total number of postings (pairs term/document). */
	private long totPostings;

	/** The total number of documents. */
	private long totDocuments;

	/** Maximum occurrence count. */
	private int maxCount;

	/** Maximum size in words of documents seen so far. */
	private int globMaxDocSize;

	/** The number of documents indexed so far in the current batch. */
	private int documentCount;

	/** The number of terms seen so far in the current batch. */
	private int numTerms;

	/** Maximum size in words of documents seen so far in the current batch. */
	int maxDocSize;

	/** The current batch. */
	private int batch;

	/** The number of occurrences in the current batch. */
	private long numOccurrences;

	/** If true, this class experienced an {@link OutOfMemoryError} during some buffer reallocation. */
	public boolean outOfMemoryError;

	/** Whether {@link #indexingType} is {@link IndexingType#STANDARD}. */
	private final boolean indexingIsStandard;
	
	/** Whether {@link #indexingType} is {@link IndexingType#REMAPPED}. */
	private final boolean indexingIsRemapped;

	/** Whether {@link #indexingType} is {@link IndexingType#VIRTUAL}. */
	private final boolean indexingIsVirtual;

	/** The number of occurrences generated by the current document. */
	private int occsInCurrDoc;

	/** A big array containing the current maximum size for each document, if the field indexed is virtual. */
	protected int[][] currSize;

	/**
	 * The maximum document pointer ever seen (could be different from the last document indexed if
	 * {@link #indexingType} is not {@link IndexingType#STANDARD}).
	 */
	private long maxDocInBatch;

	/** The width of the artificial gap introduced between virtual-document fragments. */
	protected int virtualDocumentGap;

	/** A builder that will be used to zip the document sequence while we pass through it. */
	private final DocumentCollectionBuilder builder;

	/**
	 * The cutpoints of the batches (for building later a
	 * {@link it.unimi.di.big.mg4j.index.cluster.ContiguousDocumentalStrategy}).
	 */
	protected final LongArrayList cutPoints;
	/** The completeness level required to this instance. */
	private final Completeness completeness;


	/**
	 * Creates a new scanner instance using the {@link IOFactory#FILESYSTEM_FACTORY}.
	 * 
	 * @param basename the basename (usually a global filename followed by the field name, separated
	 * by a dash).
	 * @param field the field to be indexed.
	 * @param termProcessor the term processor for this index.
	 * @param documentsAreInOrder if true, documents will be served in increasing order.
	 * @param bufferSize the buffer size used in all I/O.
	 * @param builder a builder used to create a compressed document collection on the fly.
	 * @param batchDir a directory for batch files; batch names will be relativised to this
	 * directory if it is not {@code null}.
	 */
	@Deprecated
	public Scan( final String basename, final String field, final TermProcessor termProcessor, final boolean documentsAreInOrder, final int bufferSize, final DocumentCollectionBuilder builder,
			final File batchDir ) throws IOException {
		this( basename, field, Completeness.POSITIONS, termProcessor, documentsAreInOrder ? IndexingType.STANDARD : IndexingType.VIRTUAL, 0, 0, bufferSize, builder, batchDir );
	}

	/**
	 * Creates a new scanner instance.
	 * 
	 */
	@Deprecated
	public Scan( final String basename, final String field, final TermProcessor termProcessor, final IndexingType indexingType, final int bufferSize, final DocumentCollectionBuilder builder,
			final File batchDir ) throws IOException {
		this( basename, field, Completeness.POSITIONS, termProcessor, indexingType, 0, 0, bufferSize, builder, batchDir );
	}


	/**
	 * Creates a new scanner instance using the {@link IOFactory#FILESYSTEM_FACTORY}.
	 * 
	 * @param basename the basename (usually a global filename followed by the field name, separated
	 * by a dash).
	 * @param field the field to be indexed.
	 * @param termProcessor the term processor for this index.
	 * @param indexingType the type of indexing procedure.
	 * @param numVirtualDocs the number of virtual documents that will be used, in case of a virtual
	 * index; otherwise, immaterial.
	 * @param virtualDocumentGap the artificial gap introduced between virtual documents fragments, in case
	 * of a virtual index; otherwise, immaterial.
	 * @param bufferSize the buffer size used in all I/O.
	 * @param builder a builder used to create a compressed document collection on the fly.
	 * @param batchDir a directory for batch files; batch names will be relativised to this
	 * directory if it is not {@code null}.
	 */
	public Scan( final String basename, final String field, final Completeness completeness, final TermProcessor termProcessor, final IndexingType indexingType, final int numVirtualDocs, final int virtualDocumentGap, final int bufferSize,
			final DocumentCollectionBuilder builder, final File batchDir ) throws IOException {
		this( IOFactory.FILESYSTEM_FACTORY, basename, field, completeness, termProcessor, indexingType, numVirtualDocs, virtualDocumentGap, bufferSize, builder, batchDir );
	}

	/**
	 * Creates a new scanner instance.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param basename the basename (usually a global filename followed by the field name, separated
	 * by a dash).
	 * @param field the field to be indexed.
	 * @param termProcessor the term processor for this index.
	 * @param indexingType the type of indexing procedure.
	 * @param numVirtualDocs the number of virtual documents that will be used, in case of a virtual
	 * index; otherwise, immaterial.
	 * @param virtualDocumentGap the artificial gap introduced between virtual documents fragments, in case
	 * of a virtual index; otherwise, immaterial.
	 * @param bufferSize the buffer size used in all I/O.
	 * @param builder a builder used to create a compressed document collection on the fly.
	 * @param batchDir a directory for batch files; batch names will be relativised to this
	 * directory if it is not {@code null}.
	 */
	public Scan( final IOFactory ioFactory, final String basename, final String field, final Completeness completeness, final TermProcessor termProcessor, final IndexingType indexingType, final long numVirtualDocs, final int virtualDocumentGap, final int bufferSize,
			final DocumentCollectionBuilder builder, final File batchDir ) throws IOException {
		this.ioFactory = ioFactory;
		this.basename = basename;
		this.field = field;
		this.completeness = completeness;
		this.termProcessor = termProcessor;
		this.bufferSize = bufferSize;
		this.builder = builder;
		this.batchDir = batchDir;
		this.virtualDocumentGap = virtualDocumentGap;
		this.cutPoints = new LongArrayList();
		this.cutPoints.add( 0 );

		termMap = new Object2ReferenceOpenHashMap<MutableString, ByteArrayPostingList>( INITIAL_TERM_MAP_SIZE );

		flags = new EnumMap<Component, Coding>( CompressionFlags.DEFAULT_STANDARD_INDEX );
		if ( completeness.compareTo( Completeness.POSITIONS ) < 0 ) flags.remove( Component.POSITIONS );
		if ( completeness.compareTo( Completeness.COUNTS ) < 0 ) flags.remove( Component.COUNTS );

		indexingIsStandard = indexingType == IndexingType.STANDARD;
		indexingIsRemapped = indexingType == IndexingType.REMAPPED;
		indexingIsVirtual = indexingType == IndexingType.VIRTUAL;
		if ( indexingIsVirtual && virtualDocumentGap == 0 ) throw new IllegalArgumentException( "Illegal virtual document gap: " + virtualDocumentGap );
		
		if ( indexingIsVirtual ) currSize = IntBigArrays.newBigArray( numVirtualDocs );
		maxDocInBatch = ( currSize != null ? IntBigArrays.length( currSize ) : 0 ) -1;
		openSizeBitStream();
	}

	/** Cleans all intermediate files generated by a run of this class.
	 *
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param basename the basename of the run.
	 * @param batches the number of generated batches.
	 * @param batchDir if not {@code null}, a temporary directory where the batches are located.
	 */
	public static void cleanup( final IOFactory ioFactory, final String basename, final int batches, final File batchDir ) throws IOException {
		final String basepath = ( batchDir != null ? new File( basename ) : new File( basename ) ).getCanonicalPath();
		ioFactory.delete( basepath.toString() + CLUSTER_STRATEGY_EXTENSION );
		ioFactory.delete( basepath.toString()  + CLUSTER_PROPERTIES_EXTENSION );
		for( int i = 0; i < batches; i++ ) {
			final String batchBasename = batchBasename( i, basename, batchDir );
			ioFactory.delete( batchBasename + DiskBasedIndex.FREQUENCIES_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.OCCURRENCIES_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.INDEX_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.OFFSETS_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.SIZES_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.STATS_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.TERMS_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION );
			ioFactory.delete( batchBasename + DiskBasedIndex.UNSORTED_TERMS_EXTENSION );
		}
	}
	
	/**
	 * Returns the name of a batch.
	 * 
	 * <p>You can override this method if you prefer a different batch naming scheme.
	 * 
	 * @param batch the batch number.
	 * @param basename the index basename.
	 * @param batchDir if not {@code null}, a temporary directory for batches.
	 * @return simply <code>basename@batch</code>, if <code>batchDir</code> is
	 * {@code null}; otherwise, we relativise the name to <code>batchDir</code>.
	 */
	protected static String batchBasename( int batch, String basename, final File batchDir ) {
		return batchDir != null ? new File( batchDir, basename + "@" + batch ).toString() : basename + "@" + batch;
	}


	/**
	 * Dumps the current batch on disk as an index.
	 *
	 * @return the number of occurrences contained in the batch. 
	 */
	protected long dumpBatch() throws IOException, ConfigurationException {

		outOfMemoryError = false;
		final String batchBasename = batchBasename( batch, basename, batchDir );
		LOGGER.debug( "Generating index " + batchBasename + "; documents: " + documentCount + "; terms: " + numTerms + "; occurrences: " + numOccurrences );

		// This is not strictly necessary, but nonetheless it frees enough memory for the subsequent allocation. 
		for( ByteArrayPostingList bapl: termMap.values() ) bapl.close();
		// We write down all term in appearance order in termArray.
		MutableString[] termArray = termMap.keySet().toArray( new MutableString[ numTerms ] );

		if ( ASSERTS ) assert numTerms == termMap.size();
		if ( ! indexingIsVirtual ) sizes.close();

		// We sort the terms appearing in the batch and write them on disk.
		Arrays.sort( termArray );
		final PrintWriter pw = new PrintWriter( new OutputStreamWriter( new FastBufferedOutputStream( ioFactory.getOutputStream( batchBasename + DiskBasedIndex.TERMS_EXTENSION ), bufferSize ), "UTF-8" ) );
		for ( MutableString t : termArray ) t.println( pw );
		pw.close();

		try {
			if ( indexingIsStandard ) {
				/* For standard indexing, we exploit shamelessly the fact that the bistreams stored in memory
				 * are compatible with BitStreamIndexWriter's format, dumping directly the bitstreams on
				 * disk and simulating everything that a BitStreamIndexWriter would do. */
				
				final OutputBitStream index = new OutputBitStream( ioFactory.getOutputStream( batchBasename + DiskBasedIndex.INDEX_EXTENSION ), false );
				final OutputBitStream offsets = new OutputBitStream( ioFactory.getOutputStream( batchBasename + DiskBasedIndex.OFFSETS_EXTENSION ), false );
				final OutputBitStream posNumBits = new OutputBitStream( ioFactory.getOutputStream( batchBasename + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION ), false );
				final OutputBitStream sumsMaxPos = new OutputBitStream( ioFactory.getOutputStream( batchBasename + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION ), false );
				final OutputBitStream frequencies = new OutputBitStream( ioFactory.getOutputStream( batchBasename + DiskBasedIndex.FREQUENCIES_EXTENSION ), false );
				final OutputBitStream occurrencies = new OutputBitStream( ioFactory.getOutputStream( batchBasename + DiskBasedIndex.OCCURRENCIES_EXTENSION ), false );

				ByteArrayPostingList baps;
				int maxCount = 0;
				long frequency;
				long bitLength, postings = 0, prevOffset = 0;

				offsets.writeGamma( 0 );

				for ( int i = 0; i < numTerms; i++ ) {
					baps = termMap.get( termArray[ i ] );
					frequency = baps.frequency;

					if ( maxCount < baps.maxCount ) maxCount = baps.maxCount;
					bitLength = baps.writtenBits();
					baps.align();

					postings += frequency;

					index.writeLongGamma( frequency - 1 );
	
					// We need special treatment for terms appearing in all documents
					if ( frequency == documentCount ) baps.stripPointers( index, bitLength );
					else index.write( baps.buffer, bitLength );

					frequencies.writeLongGamma( frequency );
					occurrencies.writeLongGamma( baps.occurrency );
					offsets.writeLongGamma( index.writtenBits() - prevOffset );
					posNumBits.writeLongGamma( baps.posNumBits );
					sumsMaxPos.writeLongDelta( baps.sumMaxPos );
					prevOffset = index.writtenBits();
				}

				totPostings += postings;
				if ( this.maxCount < maxCount ) this.maxCount = maxCount;

				final Properties properties = new Properties();
				properties.setProperty( Index.PropertyKeys.DOCUMENTS, documentCount );
				properties.setProperty( Index.PropertyKeys.TERMS, numTerms );
				properties.setProperty( Index.PropertyKeys.POSTINGS, postings );
				properties.setProperty( Index.PropertyKeys.MAXCOUNT, maxCount );
				properties.setProperty( Index.PropertyKeys.INDEXCLASS, FileIndex.class.getName() );
				properties.addProperty( Index.PropertyKeys.CODING, "FREQUENCIES:GAMMA" );
				properties.addProperty( Index.PropertyKeys.CODING, "POINTERS:DELTA" );
				if ( completeness.compareTo( Completeness.COUNTS ) >= 0 ) properties.addProperty( Index.PropertyKeys.CODING, "COUNTS:GAMMA" );
				if ( completeness.compareTo( Completeness.POSITIONS ) >= 0 ) properties.addProperty( Index.PropertyKeys.CODING, "POSITIONS:DELTA" );
				properties.setProperty( Index.PropertyKeys.TERMPROCESSOR, ObjectParser.toSpec( termProcessor ) );
				properties.setProperty( Index.PropertyKeys.OCCURRENCES, numOccurrences );
				properties.setProperty( Index.PropertyKeys.MAXDOCSIZE, maxDocSize );
				properties.setProperty( Index.PropertyKeys.SIZE, index.writtenBits() );
				if ( field != null ) properties.setProperty( Index.PropertyKeys.FIELD, field );
				saveProperties( ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION );
				index.close();
				offsets.close();
				posNumBits.close();
				sumsMaxPos.close();
				occurrencies.close();
				frequencies.close();
			}
			else {
				final IndexWriter indexWriter = new BitStreamIndexWriter( ioFactory, batchBasename, maxDocInBatch + 1, true, flags );

				ByteArrayPostingList bapl;
				OutputBitStream obs;
				int maxCount = -1;
				int frequency;
				int maxFrequency = 0;
				// Compute max frequency and allocate position array.
				for ( ByteArrayPostingList b : termMap.values() ) {
					b.close();
					b.align();
					if ( b.frequency > Integer.MAX_VALUE ) throw new IllegalArgumentException( "Batches of non-standard indices are required to have frequency not larger than Integer.MAX_VALUE" );
					if ( maxFrequency < b.frequency ) maxFrequency = (int)b.frequency;
					if ( maxCount < b.maxCount ) maxCount = b.maxCount;
				}

				final long[] bitPos = new long[ maxFrequency ];
				final int[] pointer = new int[ maxFrequency ];
				int[] pos = new int[ maxCount ];
				final boolean hasCounts = completeness.compareTo( Completeness.COUNTS ) >= 0;
				final boolean hasPositions = completeness.compareTo( Completeness.POSITIONS ) >= 0;
				int count = -1, moreCount = -1;
				
				for ( int i = 0; i < numTerms; i++ ) {
					bapl = termMap.get( termArray[ i ] );
					@SuppressWarnings("resource")
					final InputBitStream ibs = new InputBitStream( bapl.buffer );
					frequency = (int)bapl.frequency; // This could be much more than the actual frequency in virtual indices

					// Calculate posting bit positions and corresponding pointers
					for ( int j = 0; j < frequency; j++ ) {
						bitPos[ j ] = ibs.readBits(); // Cache bit poisition
						pointer[ j ] = ibs.readDelta(); // Cache pointer
						if ( hasCounts ) count = ibs.readGamma() + 1; 
						if ( hasPositions ) ibs.skipDeltas( count ); // Skip document positions
					}

					// Sort stably pointers and positions by increasing pointer
					it.unimi.dsi.fastutil.Arrays.quickSort( 0, frequency, new AbstractIntComparator() {
						private static final long serialVersionUID = 1L;

						public int compare( final int i0, final int i1 ) {
							final int t = pointer[ i0 ] - pointer[ i1 ];
							if ( t != 0 ) return t;
							final long u = bitPos[ i0 ] - bitPos[ i1 ]; // We need a stable sort
							return u < 0 ? -1 : u > 0 ? 1 : 0;
						}
					},
					new Swapper() {
						public void swap( final int i0, final int i1 ) {
							final long t = bitPos[ i0 ]; bitPos[ i0 ] = bitPos[ i1 ]; bitPos[ i1 ] = t;
							final int p = pointer[ i0 ]; pointer[ i0 ] = pointer[ i1 ]; pointer[ i1 ] = p;
						}
					} );

					int actualFrequency = frequency;
					// Compute actual frequency for virtual indices
					if ( indexingIsVirtual ) {
						actualFrequency = 1;
						for ( int j = 1; j < frequency; j++ ) if ( pointer[ j ] != pointer[ j - 1 ] ) actualFrequency++;
						if ( ASSERTS ) {
							for ( int j = 1; j < frequency; j++ ) {
								assert pointer[ j ] >= pointer[ j - 1 ];
								assert pointer[ j ] != pointer[ j - 1 ] || bitPos[ j ] > bitPos[ j - 1 ];
							}
						}
					}

					indexWriter.newInvertedList();
					indexWriter.writeFrequency( actualFrequency );

					int currPointer;
					for ( int j = 0; j < frequency; j++ ) {
						ibs.position( bitPos[ j ] );
						obs = indexWriter.newDocumentRecord();
						indexWriter.writeDocumentPointer( obs, currPointer = ibs.readDelta() );
						if ( ASSERTS ) assert currPointer == pointer[ j ];
						if ( hasCounts ) count = ibs.readGamma() + 1;
						if ( hasPositions ) { 
							ibs.readDeltas( pos, count );
							for ( int p = 1; p < count; p++ ) pos[ p ] += pos[ p - 1 ] + 1;
						}

						if ( indexingIsVirtual ) {
							while( j < frequency - 1 ) {
								ibs.position( bitPos[ j + 1 ] );
								if ( currPointer != ibs.readDelta() ) break;
								j++;
								if ( hasCounts ) moreCount = ibs.readGamma() + 1;
								if ( hasPositions ) {
									pos = IntArrays.grow( pos, count + moreCount, count );
									pos[ count ] = ibs.readDelta();
									if ( ASSERTS ) assert pos[ count ] > pos[ count - 1 ];
									for ( int p = 1; p < moreCount; p++ ) pos[ count + p ] = pos[ count + p - 1 ] + 1 + ibs.readDelta();
								}
								count += moreCount;
							}
							if ( maxCount < count ) maxCount = count;
						}

						if ( hasCounts ) indexWriter.writePositionCount( obs, count );
						if ( hasPositions ) indexWriter.writeDocumentPositions( obs, pos, 0, count, -1 );
					}
				}

				if ( this.maxCount < maxCount ) this.maxCount = maxCount;

				indexWriter.close();
				final Properties properties = indexWriter.properties();
				totPostings += properties.getLong( "postings" );
				properties.setProperty( Index.PropertyKeys.TERMPROCESSOR, ObjectParser.toSpec( termProcessor ) );
				properties.setProperty( Index.PropertyKeys.OCCURRENCES, numOccurrences );
				properties.setProperty( Index.PropertyKeys.MAXDOCSIZE, maxDocSize );
				properties.setProperty( Index.PropertyKeys.SIZE, indexWriter.writtenBits() );
				if ( field != null ) properties.setProperty( Index.PropertyKeys.FIELD, field );
				saveProperties( ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION );

				if ( indexingIsRemapped ) {
					// We must permute sizes
					final int[] document = new int[ documentCount ], size = new int[ documentCount ];
					final InputBitStream sizes = new InputBitStream( ioFactory.getInputStream( batchBasename + DiskBasedIndex.SIZES_EXTENSION ), false );
					for ( int i = 0; i < documentCount; i++ ) {
						document[ i ] = sizes.readGamma();
						size[ i ] = sizes.readGamma();
					}
					sizes.close();
					
					it.unimi.dsi.fastutil.Arrays.quickSort( 0, documentCount, new AbstractIntComparator() {
						private static final long serialVersionUID = 1L;

						public int compare( int x, int y ) {
							return document[ x ] - document[ y ];
						}
					}, new Swapper() {
						public void swap( int x, int y ) {
							int t = document[ x ];
							document[ x ] = document[ y ];
							document[ y ] = t;
							t = size[ x ];
							size[ x ] = size[ y ];
							size[ y ] = t;
						}
					} );


					final OutputBitStream permutedSizes = new OutputBitStream( ioFactory.getOutputStream( batchBasename( batch, basename, batchDir ) + DiskBasedIndex.SIZES_EXTENSION ), false );
					for ( int i = 0, d = 0; i < documentCount; i++ ) {
						while ( d++ < document[ i ] ) permutedSizes.writeGamma( 0 );
						permutedSizes.writeGamma( size[ i ] );
					}
					permutedSizes.close();
				}
			}
			
			if ( indexingIsVirtual ) {
				final OutputBitStream sizes = new OutputBitStream( ioFactory.getOutputStream( batchBasename( batch, basename, batchDir ) + DiskBasedIndex.SIZES_EXTENSION ) );
				final long length = IntBigArrays.length( currSize );
				for ( int i = 0; i < length; i++ ) sizes.writeGamma( IntBigArrays.get( currSize, i ) );
				sizes.close();
			}

			termMap.clear();
			//termMap.trim( INITIAL_TERM_MAP_SIZE );
			//termMap.growthFactor( Hash.DEFAULT_GROWTH_FACTOR ); // In case we changed it because of an out-of-memory error.

			numTerms = 0;
			totOccurrences += numOccurrences;
			totDocuments += documentCount;
			final long result = numOccurrences;
			globMaxDocSize = Math.max( maxDocSize, globMaxDocSize );
			if ( indexingIsStandard ) cutPoints.add( cutPoints.getLong( cutPoints.size() - 1 ) + documentCount );
			numOccurrences = documentCount = maxDocSize = 0;
			maxDocInBatch = ( currSize != null ? IntBigArrays.length( currSize ) : 0 ) -1;
			batch++;

			System.gc(); // This is exactly the right time to do collection and compaction.
			return result;
		}
		catch ( IOException e ) {
			LOGGER.error( "I/O Error on batch " + batch );
			throw e;
		}
	}

	protected void openSizeBitStream() throws IOException {
		if ( ! indexingIsVirtual ) sizes = new OutputBitStream( ioFactory.getOutputStream( batchBasename( batch, basename, batchDir ) + DiskBasedIndex.SIZES_EXTENSION ), false );
	}
	
	/**
	 * Runs in parallel a number of instances, indexing positions.
	 * @see #run(String, DocumentSequence, it.unimi.di.big.mg4j.tool.Scan.Completeness, TermProcessor, DocumentCollectionBuilder, int, int, int, int[], VirtualDocumentResolver[], int[], String, long, String)
	 */
	@Deprecated
	public static void run( final String basename, final DocumentSequence documentSequence, final TermProcessor termProcessor, final DocumentCollectionBuilder builder, final int bufferSize,
			final int documentsPerBatch, @SuppressWarnings("unused") final int maxTerms, final int[] indexedField, final VirtualDocumentResolver[] virtualDocumentResolver, final int[] virtualGap, final String mapFile, final long logInterval,
			final String tempDirName ) throws ConfigurationException, IOException {
		run( basename, documentSequence, Completeness.POSITIONS, termProcessor, builder, bufferSize, documentsPerBatch, DEFAULT_MAX_TERMS, indexedField, virtualDocumentResolver, virtualGap, mapFile, logInterval, tempDirName );
	}

	/**
	 * Runs in parallel a number of instances using the {@link IOFactory#FILESYSTEM_FACTORY}.
	 * 
	 * <p>This commodity method takes care of instantiating one instance per indexed field, and to
	 * pass the right information to each instance. All options are common to all fields, except for
	 * the number of occurrences in a batch, which can be tuned for each field separately.
	 * 
	 * @param basename the index basename.
	 * @param documentSequence a document sequence.
	 * @param completeness the completeness level of this run.
	 * @param termProcessor the term processor for this index.
	 * @param builder if not {@code null}, a builder that will be used to create new collection built using <code>documentSequence</code>.
	 * @param bufferSize the buffer size used in all I/O.
	 * @param documentsPerBatch the number of documents that we should try to put in each segment.
	 * @param maxTerms the maximum number of overall (i.e., cross-field) terms in a batch.
	 * @param indexedField the fields that should be indexed, in increasing order.
	 * @param virtualDocumentResolver the array of virtual document resolvers to be used, parallel
	 * to <code>indexedField</code>: it can safely contain anything (even {@code null})
	 * in correspondence to non-virtual fields, and can safely be {@code null} if no fields
	 * are virtual.
	 * @param virtualGap the array of virtual field gaps to be used, parallel to
	 * <code>indexedField</code>: it can safely contain anything in correspondence to non-virtual
	 * fields, and can safely be {@code null} if no fields are virtual.
	 * @param mapFile the name of a file containing a map to be applied to document indices.
	 * @param logInterval the minimum time interval between activity logs in milliseconds.
	 * @param tempDirName a directory for temporary files.
	 */
	public static void run( final String basename, final DocumentSequence documentSequence, final Completeness completeness, final TermProcessor termProcessor, final DocumentCollectionBuilder builder, final int bufferSize,
			final int documentsPerBatch, final int maxTerms, final int[] indexedField, final VirtualDocumentResolver[] virtualDocumentResolver, final int[] virtualGap, final String mapFile, final long logInterval,
			final String tempDirName ) throws ConfigurationException, IOException {
		run( IOFactory.FILESYSTEM_FACTORY, basename, documentSequence, completeness, termProcessor, builder, bufferSize,
			documentsPerBatch, maxTerms, indexedField, virtualDocumentResolver, virtualGap, mapFile, logInterval, tempDirName );
	}

	/**
	 * Runs in parallel a number of instances.
	 * 
	 * <p>This commodity method takes care of instantiating one instance per indexed field, and to
	 * pass the right information to each instance. All options are common to all fields, except for
	 * the number of occurrences in a batch, which can be tuned for each field separately.
	 * 
	 * @param ioFactory the factory that will be used to perform I/O.
	 * @param basename the index basename.
	 * @param documentSequence a document sequence.
	 * @param completeness the completeness level of this run.
	 * @param termProcessor the term processor for this index.
	 * @param builder if not {@code null}, a builder that will be used to create new collection built using <code>documentSequence</code>.
	 * @param bufferSize the buffer size used in all I/O.
	 * @param documentsPerBatch the number of documents that we should try to put in each segment.
	 * @param maxTerms the maximum number of overall (i.e., cross-field) terms in a batch.
	 * @param indexedField the fields that should be indexed, in increasing order.
	 * @param virtualDocumentResolver the array of virtual document resolvers to be used, parallel
	 * to <code>indexedField</code>: it can safely contain anything (even {@code null})
	 * in correspondence to non-virtual fields, and can safely be {@code null} if no fields
	 * are virtual.
	 * @param virtualGap the array of virtual field gaps to be used, parallel to
	 * <code>indexedField</code>: it can safely contain anything in correspondence to non-virtual
	 * fields, and can safely be {@code null} if no fields are virtual.
	 * @param mapFile the name of a file containing a map to be applied to document indices.
	 * @param logInterval the minimum time interval between activity logs in milliseconds.
	 * @param tempDirName a directory for temporary files.
	 * @throws IOException
	 * @throws ConfigurationException
	 */
	@SuppressWarnings("unchecked")
	public static void run( final IOFactory ioFactory, final String basename, final DocumentSequence documentSequence, final Completeness completeness, final TermProcessor termProcessor, final DocumentCollectionBuilder builder, final int bufferSize,
			final int documentsPerBatch, final int maxTerms, final int[] indexedField, final VirtualDocumentResolver[] virtualDocumentResolver, final int[] virtualGap, final String mapFile, final long logInterval,
			final String tempDirName ) throws ConfigurationException, IOException {

		final boolean building = builder != null;
		final int numberOfIndexedFields = indexedField.length;
		if ( numberOfIndexedFields == 0 ) throw new IllegalArgumentException( "You must specify at least one field" );
		final DocumentFactory factory = documentSequence.factory();
		final File tempDir = tempDirName == null ? null : new File( tempDirName );
		for ( int i = 0; i < indexedField.length; i++ )
			if ( factory.fieldType( indexedField[ i ] ) == DocumentFactory.FieldType.VIRTUAL && ( virtualDocumentResolver == null || virtualDocumentResolver[ i ] == null ) ) throw new IllegalArgumentException(
					"No resolver was associated with virtual field " + factory.fieldName( indexedField[ i ] ) );

		if ( mapFile != null && ioFactory != IOFactory.FILESYSTEM_FACTORY ) throw new IllegalStateException( "Remapped indices currently do not support I/O factories" );
		final int[] map = mapFile != null ? BinIO.loadInts( mapFile ) : null;

		final Scan[] scan = new Scan[ numberOfIndexedFields ]; // To scan textual content
		final PayloadAccumulator[] accumulator = new PayloadAccumulator[ numberOfIndexedFields ]; // To accumulate
		// document data

		final ProgressLogger pl = new ProgressLogger( LOGGER, logInterval, TimeUnit.MILLISECONDS, "documents" );
		if ( documentSequence instanceof DocumentCollection ) pl.expectedUpdates = ( (DocumentCollection)documentSequence ).size();
	
		for ( int i = 0; i < numberOfIndexedFields; i++ ) {
			final String fieldName = factory.fieldName( indexedField[ i ] );
			switch ( factory.fieldType( indexedField[ i ] ) ) {
			case TEXT:
				scan[ i ] = new Scan( ioFactory, basename + '-' + fieldName, fieldName, completeness, termProcessor, map != null ? IndexingType.REMAPPED
						: IndexingType.STANDARD, 0, 0, bufferSize, builder, tempDir );
				break;
			case VIRTUAL:
				scan[ i ] = new Scan( ioFactory, basename + '-' + fieldName, fieldName, completeness, termProcessor, IndexingType.VIRTUAL,
						virtualDocumentResolver[ i ].numberOfDocuments(), virtualGap[ i ], bufferSize, builder, tempDir );
				break;

			case DATE:
				accumulator[ i ] = new PayloadAccumulator( ioFactory, basename + '-' + fieldName, new DatePayload(), fieldName,
						map != null ? IndexingType.REMAPPED : IndexingType.STANDARD, documentsPerBatch, tempDir );
				break;
			case INT:
				accumulator[ i ] = new PayloadAccumulator( ioFactory, basename + '-' + fieldName, new IntegerPayload(), fieldName,
						map != null ? IndexingType.REMAPPED : IndexingType.STANDARD, documentsPerBatch, tempDir );
				break;
			default:

			}
		}

		if ( building ) builder.open( "@0" ); // First batch
		
		pl.displayFreeMemory = true;
		pl.start( "Indexing documents..." );

		DocumentIterator iterator = documentSequence.iterator();
		Reader reader;
		WordReader wordReader;
		List<VirtualDocumentFragment> fragments;
		Document document;

		int documentPointer = 0, documentsInBatch = 0;
		long batchStartTime = System.currentTimeMillis();
		boolean outOfMemoryError = false;
		final MutableString title = new MutableString();

		while ( ( document = iterator.nextDocument() ) != null ) {
			
			long overallTerms = 0;
			if ( document.title() != null ) {
				title.replace( document.title() );
				title.replace( ScanMetadata.LINE_TERMINATORS, ScanMetadata.SPACES );
			}
			if ( building ) builder.startDocument( document.title(), document.uri() );
			for ( int i = 0; i < numberOfIndexedFields; i++ ) {
				switch ( factory.fieldType( indexedField[ i ] ) ) {
				case TEXT:
					reader = (Reader)document.content( indexedField[ i ] );
					wordReader = document.wordReader( indexedField[ i ] );
					wordReader.setReader( reader );
					if ( building ) builder.startTextField();
					scan[ i ].processDocument( map != null ? map[ documentPointer ] : documentPointer, wordReader );
					if ( building ) builder.endTextField();
					overallTerms += scan[ i ].numTerms;
					break;
				case VIRTUAL:
					fragments = (List<VirtualDocumentFragment>)document.content( indexedField[ i ] );
					wordReader = document.wordReader( indexedField[ i ] );
					virtualDocumentResolver[ i ].context( document );
					for( VirtualDocumentFragment fragment: fragments ) {
						long virtualDocumentPointer = virtualDocumentResolver[ i ].resolve( fragment.documentSpecifier() );
						if ( virtualDocumentPointer < 0 ) continue;
						// ALERT: we must rewrite remapping to work with long-sized document pointers.
						if ( map != null ) virtualDocumentPointer = map[ (int)virtualDocumentPointer ];
						wordReader.setReader( new FastBufferedReader( fragment.text() ) );
						scan[ i ].processDocument( (int)virtualDocumentPointer, wordReader );
					}
					if ( building ) builder.virtualField( fragments );
					overallTerms += scan[ i ].numTerms;
					break;
				default:
					Object o = document.content( indexedField[ i ] );
					accumulator[ i ].processData( map != null ? map[ documentPointer ] : documentPointer, o );
					if ( building ) builder.nonTextField( o );
					break;
				}

				if ( scan[ i ] != null && scan[ i ].outOfMemoryError ) outOfMemoryError = true;
			}
			if ( building ) builder.endDocument();
			documentPointer++;
			documentsInBatch++;
			document.close();
			pl.update();

			long percAvailableMemory = 100;
			boolean compacted = false;
			if ( ( documentPointer & 0xFF ) == 0 ) {
				// We try compaction if we detect less than PERC_AVAILABLE_MEMORY_CHECK memory available
				percAvailableMemory = Util.percAvailableMemory();
				if ( ! outOfMemoryError && percAvailableMemory < PERC_AVAILABLE_MEMORY_CHECK ) {
					LOGGER.info( "Starting compaction... (" + percAvailableMemory + "% available)" );
					compacted = true;
					Util.compactMemory();
					percAvailableMemory = Util.percAvailableMemory();
					LOGGER.info( "Compaction completed (" + percAvailableMemory + "% available)" );
				}
			}
			
			if ( outOfMemoryError || overallTerms >= maxTerms || documentsInBatch == documentsPerBatch || ( compacted && percAvailableMemory < PERC_AVAILABLE_MEMORY_DUMP ) ) {
				if ( outOfMemoryError ) LOGGER.warn( "OutOfMemoryError during buffer reallocation: writing a batch of " + documentsInBatch + " documents" );
				else if ( overallTerms >= maxTerms ) LOGGER.warn( "Too many terms (" + overallTerms + "): writing a batch of " + documentsInBatch + " documents" );
				else if ( compacted && percAvailableMemory < PERC_AVAILABLE_MEMORY_DUMP ) LOGGER.warn( "Available memory below " + PERC_AVAILABLE_MEMORY_DUMP + "%: writing a batch of " + documentsInBatch + " documents" );

				long occurrences = 0;
				for ( int i = 0; i < numberOfIndexedFields; i++ ) {
					switch ( factory.fieldType( indexedField[ i ] ) ) {
					case TEXT:
					case VIRTUAL:
						occurrences += scan[ i ].dumpBatch();
						scan[ i ].openSizeBitStream();
						break;
					default:
						accumulator[ i ].writeData();
					}
				}
				
				if ( building ) {
					builder.close();
					builder.open( "@" + scan[ 0 ].batch );
				}

				LOGGER.info( "Last set of batches indexed at " + Util.format( ( 1000. * occurrences ) / ( System.currentTimeMillis() - batchStartTime ) ) + " occurrences/s" );
				batchStartTime = System.currentTimeMillis();
				documentsInBatch = 0;
				outOfMemoryError = false;
			}
		}

		iterator.close();
		if ( builder != null ) builder.close();

		for ( int i = 0; i < numberOfIndexedFields; i++ ) {
			switch ( factory.fieldType( indexedField[ i ] ) ) {
			case TEXT:
			case VIRTUAL:
				scan[ i ].close();
				break;
			default:
				accumulator[ i ].close();
				break;
			}

		}

		documentSequence.close();
		
		pl.done();

		if ( building ) {
			final String name = new File( builder.basename() ).getName();
			final String[] collectionName = new String[ scan[ 0 ].batch ];
			for ( int i = scan[ 0 ].batch; i-- != 0; ) collectionName[ i ] = name + "@" + i + DocumentCollection.DEFAULT_EXTENSION;
			IOFactories.storeObject( ioFactory, new ConcatenatedDocumentCollection( collectionName ), builder.basename() + DocumentCollection.DEFAULT_EXTENSION );
		}

		if ( map != null && documentPointer != map.length ) LOGGER.warn( "The document sequence contains " + documentPointer + " documents, but the map contains "
				+ map.length + " integers" );
	}


	final MutableString word = new MutableString();

	final MutableString nonWord = new MutableString();

	/** The default delimiter separating two documents read from standard input (a newline). */
	public static final int DEFAULT_DELIMITER = 10;

	/** The default batch size. */
	public static final int DEFAULT_BATCH_SIZE = 100000;

	/** The default maximum number of terms. */
	public static final int DEFAULT_MAX_TERMS = 10000000;

	/** The default buffer size. */
	public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

	/** The default virtual field gap. */
	public static final int DEFAULT_VIRTUAL_DOCUMENT_GAP = 64;

	/**
	 * Processes a document.
	 * 
	 * @param documentPointer the integer pointer associated with the document.
	 * @param wordReader the word reader associated with the document.
	 */
	public void processDocument( final long documentPointer, final WordReader wordReader ) throws IOException {
		int pos = indexingIsVirtual ? IntBigArrays.get( currSize, documentPointer ) : 0;
		final long actualPointer = indexingIsStandard ? documentCount : documentPointer;
		ByteArrayPostingList termBapl;

		word.length( 0 );
		nonWord.length( 0 );

		while ( wordReader.next( word, nonWord ) ) {
			if ( builder != null ) builder.add( word, nonWord );
			if ( word.length() == 0 ) continue;
			if ( !termProcessor.processTerm( word ) ) {
				pos++; // We do consider the positions of terms canceled out by the term processor.
				continue;
			}

			// We check whether we have already seen this term. If not, we add it to the term map.
			if ( ( termBapl = termMap.get( word ) ) == null ) {
				try {
					termBapl = new ByteArrayPostingList( new byte[ BYTE_ARRAY_POSTING_LIST_INITIAL_SIZE ], indexingIsStandard, completeness );
					termMap.put( word.copy(), termBapl );
				}
				catch( OutOfMemoryError e ) {
					/* There is not enough memory for enlarging the table. We set a very low growth factor, so at
					 * the next put() the enlargement will likely succeed. If not, we will generate several
					 * out-of-memory error, but we should get to the end anyway, and we will 
					 * dump the current batch as soon as the current document is finished. */
					outOfMemoryError = true;
					//termMap.growthFactor( 1 );
				}
				numTerms++;
				if ( numTerms % TERM_REPORT_STEP == 0 ) LOGGER.info( "[" + Util.format( numTerms ) + " term(s)]" );
			}

			// We now record the occurrence. If a renumbering map has
			// been specified, we have to renumber the document index through it.
			termBapl.setDocumentPointer( actualPointer );
			termBapl.addPosition( pos );
			// Record whether this posting list has an out-of-memory-error problem.
			if ( termBapl.outOfMemoryError ) outOfMemoryError = true;
			occsInCurrDoc++;
			numOccurrences++;
			pos++;
		}

		if ( pos > maxDocSize ) maxDocSize = pos;
		
		if ( indexingIsStandard ) sizes.writeGamma( pos );
		else if ( indexingIsRemapped ) {
			sizes.writeLongGamma( actualPointer );
			sizes.writeGamma( pos );
		}
		
		if ( indexingIsVirtual ) IntBigArrays.set( currSize, documentPointer, IntBigArrays.get( currSize, documentPointer ) + occsInCurrDoc + virtualDocumentGap );

		pos = occsInCurrDoc = 0;
		documentCount++;
		if ( actualPointer > maxDocInBatch ) maxDocInBatch = actualPointer;
	}


	private static void makeEmpty( final IOFactory ioFactory, final String filename ) throws IOException {
		if ( ioFactory.exists( filename) && ! ioFactory.delete( filename ) ) throw new IOException( "Cannot delete file " + filename );
		ioFactory.createNewFile( filename );
	}

	public static void saveProperties( IOFactory ioFactory, Properties properties, String filename ) throws ConfigurationException, IOException {
		final OutputStream propertiesOutputStream = ioFactory.getOutputStream( filename );
		properties.save( propertiesOutputStream );
		propertiesOutputStream.close();
	}
	

	/**
	 * Closes this pass, releasing all resources.
	 */
	public void close() throws ConfigurationException, IOException {
		if ( numOccurrences > 0 ) dumpBatch();

		if ( numOccurrences == 0 ){
			if ( batch == 0 ) {
				// Special case: no term has been indexed. We generate an empty batch.
				final String batchBasename = batchBasename( 0, basename, batchDir );
				LOGGER.debug( "Generating empty index " + batchBasename );
				makeEmpty( ioFactory, batchBasename + DiskBasedIndex.TERMS_EXTENSION );
				makeEmpty( ioFactory, batchBasename + DiskBasedIndex.FREQUENCIES_EXTENSION );
				makeEmpty( ioFactory, batchBasename + DiskBasedIndex.OCCURRENCIES_EXTENSION );
				if ( ! indexingIsVirtual ) sizes.close();

				final IndexWriter indexWriter = new BitStreamIndexWriter( ioFactory, batchBasename, totDocuments, true, flags );
				indexWriter.close();
				final Properties properties = indexWriter.properties();
				properties.setProperty( Index.PropertyKeys.TERMPROCESSOR, ObjectParser.toSpec( termProcessor ) );
				properties.setProperty( Index.PropertyKeys.OCCURRENCES, 0 );
				properties.setProperty( Index.PropertyKeys.MAXCOUNT, 0 );
				properties.setProperty( Index.PropertyKeys.MAXDOCSIZE, maxDocSize );
				properties.setProperty( Index.PropertyKeys.SIZE, 0 );
				if ( field != null ) properties.setProperty( Index.PropertyKeys.FIELD, field );
				saveProperties( ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION );
				batch = 1;
			}
			else ioFactory.delete( batchBasename( batch, basename, batchDir ) + DiskBasedIndex.SIZES_EXTENSION ); // When there is a batch but no documents.
		}

		termMap = null;

		final Properties properties = new Properties();
		if ( field != null ) properties.setProperty( Index.PropertyKeys.FIELD, field );
		properties.setProperty( Index.PropertyKeys.BATCHES, batch );
		properties.setProperty( Index.PropertyKeys.DOCUMENTS, totDocuments );
		properties.setProperty( Index.PropertyKeys.MAXDOCSIZE, globMaxDocSize );
		properties.setProperty( Index.PropertyKeys.MAXCOUNT, maxCount );
		properties.setProperty( Index.PropertyKeys.OCCURRENCES, totOccurrences );
		properties.setProperty( Index.PropertyKeys.POSTINGS, totPostings );
		properties.setProperty( Index.PropertyKeys.TERMPROCESSOR, termProcessor.getClass().getName() );

		if ( ! indexingIsVirtual ) {
			// This set of batches can be seen as a documental cluster index.
			final Properties clusterProperties = new Properties();
			clusterProperties.addAll( properties );
			clusterProperties.setProperty( Index.PropertyKeys.TERMS, -1 );
			clusterProperties.setProperty( DocumentalCluster.PropertyKeys.BLOOM, false );
			clusterProperties.setProperty( IndexCluster.PropertyKeys.FLAT, false );

			if ( indexingIsStandard ) {
				clusterProperties.setProperty( Index.PropertyKeys.INDEXCLASS, DocumentalConcatenatedCluster.class.getName() );
				IOFactories.storeObject( ioFactory, new ContiguousDocumentalStrategy( cutPoints.toLongArray() ), basename + CLUSTER_STRATEGY_EXTENSION );
			}
			else { // Remapped
				clusterProperties.setProperty( Index.PropertyKeys.INDEXCLASS, DocumentalMergedCluster.class.getName() );
				IOFactories.storeObject( ioFactory, new IdentityDocumentalStrategy( batch, totDocuments ), basename + CLUSTER_STRATEGY_EXTENSION );
			}
			clusterProperties.setProperty( IndexCluster.PropertyKeys.STRATEGY, basename + CLUSTER_STRATEGY_EXTENSION );
			for ( int i = 0; i < batch; i++ )
				clusterProperties.addProperty( IndexCluster.PropertyKeys.LOCALINDEX, batchBasename( i, basename, batchDir ) );
			saveProperties( ioFactory, clusterProperties, basename + CLUSTER_PROPERTIES_EXTENSION );

		}

		saveProperties( ioFactory, properties, basename + DiskBasedIndex.PROPERTIES_EXTENSION );
	}

	public String toString() {
		return this.getClass().getSimpleName() + "(" + basename + ":" + field + ")";
	}

	/**
	 * An accumulator for payloads.
	 * 
	 * <P>This class is essentially a stripped-down version of {@link Scan} that just accumulate
	 * payloads in a bitstream and releases them in batches. The main difference is that neither
	 * sizes nor occurrencies are saved (as they would not make much sense).
	 */

	protected static class PayloadAccumulator {
		/** The I/O factory that will be used to create files. */
		private final IOFactory ioFactory;

		/** The current basename of the overall index (usually some basename postfixed with the field name). */
		private final String basename;

		/** The field name, if available. */
		private final String field;

		/** The total number of postings (pairs term/document). */
		private long totPostings;

		/** The directory where batches files will be created. */
		private final File batchDir;

		/** The flag map for batches. */
		final Map<Component, Coding> flags;

		/** The total number of documents. */
		private int totDocuments;

		/** The number of documents indexed so far in the current batch. */
		private int documentCount;

		/** The current batch. */
		private int batch;

		/** The type of indexing for this scan. */
		private final IndexingType indexingType;

		/** The pointers into the stream, if {@link #indexingType} is {@link IndexingType#REMAPPED}. */
		private long position[];

		/** The output stream underlying this accumulator. */
		private FastByteArrayOutputStream accumulatorStream;

		/** The accumulating output bit stream, wrapping {@link #accumulatorStream}. */
		private OutputBitStream accumulator;

		/**
		 * The cutpoints of the batches (for building later a
		 * {@link it.unimi.di.big.mg4j.index.cluster.ContiguousDocumentalStrategy}).
		 */
		protected final LongArrayList cutPoints;

		/** The payload accumulated by this accumulator. */
		private final Payload payload;

		/** The maximum document ever seen in the current batch. */
		private int maxDocInBatch;


		/**
		 * Creates a new accumulator.
		 * 
		 * @param ioFactory the factory that will be used to perform I/O.
		 * @param basename the basename (usually a global filename followed by the field name,
		 * separated by a dash).
		 * @param payload the payload stored by this accumulator.
		 * @param field the name of the accumulated field.
		 * @param indexingType the type of indexing procedure.
		 * @param documentsPerBatch the number of documents in each batch.
		 * @param batchDir a directory for batch files; batch names will be relativised to this
		 * directory if it is not {@code null}.
		 */
		public PayloadAccumulator( final IOFactory ioFactory, final String basename, final Payload payload, final String field, final IndexingType indexingType, final int documentsPerBatch, final File batchDir ) {
			this.basename = basename;
			this.ioFactory = ioFactory;
			this.payload = payload;
			this.field = field;
			this.indexingType = indexingType;
			if ( indexingType != IndexingType.STANDARD && indexingType != IndexingType.REMAPPED ) throw new UnsupportedOperationException( "Non-standard payload-based indices support only standard and remapped indexing" );
			if ( indexingType == IndexingType.REMAPPED ) position = new long[ documentsPerBatch ];
			this.batchDir = batchDir;
			this.cutPoints = new LongArrayList();
			this.cutPoints.add( 0 );

			flags = new EnumMap<Component, Coding>( CompressionFlags.DEFAULT_PAYLOAD_INDEX );
			accumulatorStream = new FastByteArrayOutputStream();
			accumulator = new OutputBitStream( accumulatorStream );
		}

		/** Writes in compressed form the data currently accumulated. */
		protected void writeData() throws IOException, ConfigurationException {

			final String batchBasename = batchBasename( batch, basename, batchDir );

			LOGGER.debug( "Generating index " + batchBasename + "; documents: " + documentCount );

			try {
				accumulator.flush();
				final InputBitStream ibs = new InputBitStream( accumulatorStream.array );
				final IndexWriter indexWriter = new BitStreamIndexWriter( ioFactory, batchBasename, indexingType == IndexingType.STANDARD ? documentCount : maxDocInBatch + 1, true, flags );
				indexWriter.newInvertedList();
				indexWriter.writeFrequency( documentCount );
				OutputBitStream obs;

				if ( indexingType == IndexingType.STANDARD ) {
					for ( int i = 0; i < documentCount; i++ ) {
						obs = indexWriter.newDocumentRecord();
						indexWriter.writeDocumentPointer( obs, i );
						payload.read( ibs );
						indexWriter.writePayload( obs, payload );
					}
				}
				else {
					// We sort position by pointed document pointer.
					LongArrays.quickSort( position, 0, documentCount, new AbstractLongComparator() {
						private static final long serialVersionUID = 1L;

						public int compare( final long position0, final long position1 ) {
							try {
								ibs.position( position0 );
								final int d0 = ibs.readDelta();
								ibs.position( position1 );
								return d0 - ibs.readDelta();
							}
							catch ( IOException e ) {
								throw new RuntimeException( e );
							}
						}
					} );
					for ( int i = 0; i < documentCount; i++ ) {
						obs = indexWriter.newDocumentRecord();
						ibs.position( position[ i ] );
						indexWriter.writeDocumentPointer( obs, ibs.readDelta() );
						payload.read( ibs );
						indexWriter.writePayload( obs, payload );
					}
				}

				indexWriter.close();

				final Properties properties = indexWriter.properties();
				totPostings += properties.getLong( "postings" );
				properties.setProperty( Index.PropertyKeys.OCCURRENCES, -1 );
				properties.setProperty( Index.PropertyKeys.MAXDOCSIZE, -1 );
				properties.setProperty( Index.PropertyKeys.SIZE, indexWriter.writtenBits() );
				properties.setProperty( Index.PropertyKeys.TERMPROCESSOR, NullTermProcessor.class.getName() );
				properties.setProperty( Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName() );
				if ( field != null ) properties.setProperty( Index.PropertyKeys.FIELD, field );
				saveProperties( ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION );

				// We *must* generate a fake term file, or index combination won't work.
				final PrintWriter termWriter = new PrintWriter( ioFactory.getOutputStream( batchBasename + DiskBasedIndex.TERMS_EXTENSION ) );
				termWriter.println( "#" );
				termWriter.close();

				cutPoints.add( cutPoints.getLong( cutPoints.size() - 1 ) + documentCount );
				accumulatorStream.reset();
				accumulator.writtenBits( 0 );
				documentCount = 0;
				maxDocInBatch = -1;
				batch++;
			}
			catch ( IOException e ) {
				LOGGER.error( "I/O Error on batch " + batch );
				throw e;
			}
		}

		/**
		 * Processes the payload of a given document.
		 * 
		 * @param documentPointer the document pointer.
		 * @param content the payload.
		 */
		public void processData( final int documentPointer, final Object content ) throws IOException {
			// We write document pointers only for non-standard indices.
			if ( indexingType != IndexingType.STANDARD ) {
				position[ documentCount ] = accumulator.writtenBits();
				accumulator.writeDelta( documentPointer );
			}
			// TODO: devise an out-of-memory-error check mechanism similar to that of ByteArrayPostingList.
			payload.set( content );
			payload.write( accumulator );

			if ( documentPointer > maxDocInBatch ) maxDocInBatch = documentPointer;
			documentCount++;
			totDocuments++;
		}

		/**
		 * Closes this accumulator, releasing all resources.
		 */
		public void close() throws ConfigurationException, IOException {
			if ( documentCount > 0 ) writeData();

			if ( totDocuments == 0 ) {
				// Special case: no document has been indexed. We generate an empty batch.
				final String batchBasename = batchBasename( 0, basename, batchDir );
				LOGGER.debug( "Generating empty index " + batchBasename );

				final IndexWriter indexWriter = new BitStreamIndexWriter( ioFactory, batchBasename, 0, true, flags );
				indexWriter.close();
				final Properties properties = indexWriter.properties();
				properties.setProperty( Index.PropertyKeys.SIZE, 0 );
				properties.setProperty( Index.PropertyKeys.OCCURRENCES, -1 );
				properties.setProperty( Index.PropertyKeys.MAXCOUNT, -1 );
				properties.setProperty( Index.PropertyKeys.MAXDOCSIZE, -1 );
				properties.setProperty( Index.PropertyKeys.TERMPROCESSOR, NullTermProcessor.class.getName() );
				properties.setProperty( Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName() );
				if ( field != null ) properties.setProperty( Index.PropertyKeys.FIELD, field );
				saveProperties( ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION );
				makeEmpty( ioFactory, batchBasename + DiskBasedIndex.TERMS_EXTENSION );
				batch = 1;
			}

			accumulator = null;
			accumulatorStream = null;
			position = null;

			final Properties properties = new Properties();
			if ( field != null ) properties.setProperty( Index.PropertyKeys.FIELD, field );
			properties.setProperty( Index.PropertyKeys.BATCHES, batch );
			properties.setProperty( Index.PropertyKeys.DOCUMENTS, totDocuments );
			properties.setProperty( Index.PropertyKeys.POSTINGS, totPostings );
			properties.setProperty( Index.PropertyKeys.OCCURRENCES, -1 );
			properties.setProperty( Index.PropertyKeys.MAXCOUNT, -1 );
			properties.setProperty( Index.PropertyKeys.MAXDOCSIZE, -1 );
			properties.setProperty( Index.PropertyKeys.TERMPROCESSOR, NullTermProcessor.class.getName() );
			properties.setProperty( Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName() );

			// This set of batches can be seen as a documental cluster index.
			final Properties clusterProperties = new Properties();
			clusterProperties.addAll( properties );
			clusterProperties.setProperty( Index.PropertyKeys.TERMS, 1 );
			clusterProperties.setProperty( IndexCluster.PropertyKeys.BLOOM, false );
			clusterProperties.setProperty( IndexCluster.PropertyKeys.FLAT, true );

			if ( indexingType == IndexingType.STANDARD ) {
				clusterProperties.setProperty( Index.PropertyKeys.INDEXCLASS, DocumentalConcatenatedCluster.class.getName() );
				IOFactories.storeObject( ioFactory, new ContiguousDocumentalStrategy( cutPoints.toLongArray() ), basename + CLUSTER_STRATEGY_EXTENSION );
			}
			else {
				clusterProperties.setProperty( Index.PropertyKeys.INDEXCLASS, DocumentalMergedCluster.class.getName() );
				IOFactories.storeObject( ioFactory, new IdentityDocumentalStrategy( batch, totDocuments ), basename + CLUSTER_STRATEGY_EXTENSION );
			}
			clusterProperties.setProperty( IndexCluster.PropertyKeys.STRATEGY, basename + CLUSTER_STRATEGY_EXTENSION );
			for ( int i = 0; i < batch; i++ )
				clusterProperties.addProperty( IndexCluster.PropertyKeys.LOCALINDEX, batchBasename( i, basename, batchDir ) );
			saveProperties( ioFactory, clusterProperties, basename + CLUSTER_PROPERTIES_EXTENSION );

			saveProperties( ioFactory, properties, basename + DiskBasedIndex.PROPERTIES_EXTENSION );
		}

	}


	public static int[] parseQualifiedSizes( final String[] qualifiedSizes, final String defaultSize, final int[] indexedField, final DocumentFactory factory ) throws ParseException {
		final int[] size = new int[ indexedField.length ];
		String defaultSpec = defaultSize;
		IntArrayList indexedFields = IntArrayList.wrap( indexedField );
		for ( int i = 0; i < qualifiedSizes.length; i++ )
			if ( qualifiedSizes[ i ].indexOf( ':' ) == -1 ) defaultSpec = qualifiedSizes[ i ];
		for ( int i = 0; i < size.length; i++ )
			size[ i ] = (int)LongSizeStringParser.parseSize( defaultSpec );
		for ( int i = 0; i < qualifiedSizes.length; i++ ) {
			final int split = qualifiedSizes[ i ].indexOf( ':' );
			if ( split >= 0 ) {
				final String fieldName = qualifiedSizes[ i ].substring( 0, split );
				final int field = factory.fieldIndex( fieldName );
				if ( field < 0 ) throw new IllegalArgumentException( "Field " + fieldName + " is not part of factory " + factory.getClass().getName() );
				if ( !indexedFields.contains( field ) ) throw new IllegalArgumentException( "Field " + factory.fieldName( field ) + " is not being indexed" );
				size[ indexedFields.indexOf( field ) ] = (int)LongSizeStringParser.parseSize( qualifiedSizes[ i ].substring( split + 1 ) );
			}
		}
		return size;
	}

	public static VirtualDocumentResolver[] parseVirtualDocumentResolver( final IOFactory ioFactory, final String[] virtualDocumentSpec, final int[] indexedField, final DocumentFactory factory ) {
		final VirtualDocumentResolver[] virtualDocumentResolver = new VirtualDocumentResolver[ indexedField.length ];
		VirtualDocumentResolver defaultResolver = null;
		IntArrayList indexedFields = IntArrayList.wrap( indexedField );
		for ( int i = 0; i < virtualDocumentSpec.length; i++ )
			if ( virtualDocumentSpec[ i ].indexOf( ':' ) == -1 ) try {
				defaultResolver = (VirtualDocumentResolver)IOFactories.loadObject( ioFactory, virtualDocumentSpec[ i ] );
			}
			catch ( IOException e ) {
				throw new RuntimeException( "An I/O error occurred while loading " + virtualDocumentSpec[ i ], e );
			}
			catch ( ClassNotFoundException e ) {
				throw new RuntimeException( "Cannot load " + virtualDocumentSpec[ i ], e );
			}
		for ( int i = 0; i < virtualDocumentResolver.length; i++ )
			virtualDocumentResolver[ i ] = defaultResolver;
		for ( int i = 0; i < virtualDocumentSpec.length; i++ ) {
			final int split = virtualDocumentSpec[ i ].indexOf( ':' );
			if ( split >= 0 ) {
				final String fieldName = virtualDocumentSpec[ i ].substring( 0, split );
				final int field = factory.fieldIndex( fieldName );
				if ( field < 0 ) throw new IllegalArgumentException( "Field " + fieldName + " is not part of factory " + factory.getClass().getName() );
				if ( !indexedFields.contains( field ) ) throw new IllegalArgumentException( "Field " + factory.fieldName( field ) + " is not being indexed" );
				if ( factory.fieldType( field ) != DocumentFactory.FieldType.VIRTUAL ) throw new IllegalArgumentException( "Field " + factory.fieldName( field ) + " is not virtual" );
				try {
					virtualDocumentResolver[ indexedFields.indexOf( field ) ] = (VirtualDocumentResolver)IOFactories.loadObject( ioFactory,virtualDocumentSpec[ i ].substring( split + 1 ) );
				}
				catch ( IOException e ) {
					throw new RuntimeException( "An I/O error occurred while loading " + virtualDocumentSpec[ i ].substring( split + 1 ), e );
				}
				catch ( ClassNotFoundException e ) {
					throw new RuntimeException( "Cannot load " + virtualDocumentSpec[ i ].substring( split + 1 ), e );
				}
			}
		}
		return virtualDocumentResolver;
	}

	public static int[] parseVirtualDocumentGap( final String[] virtualDocumentGapSpec, final int[] indexedField, final DocumentFactory factory ) {
		final int[] virtualDocumentGap = new int[ indexedField.length ];
		int defaultGap = DEFAULT_VIRTUAL_DOCUMENT_GAP;
		IntArrayList indexedFields = IntArrayList.wrap( indexedField );
		for ( int i = 0; i < virtualDocumentGapSpec.length; i++ )
			if ( virtualDocumentGapSpec[ i ].indexOf( ':' ) == -1 ) try {
				defaultGap = Integer.parseInt( virtualDocumentGapSpec[ i ] );
				if ( defaultGap < 0 ) throw new NumberFormatException( "Gap can't be negative" );
			}
			catch ( NumberFormatException e ) {
				throw new RuntimeException( "Cannot parse gap correctly " + virtualDocumentGapSpec[ i ], e );
			}
		for ( int i = 0; i < virtualDocumentGap.length; i++ )
			virtualDocumentGap[ i ] = defaultGap;
		for ( int i = 0; i < virtualDocumentGapSpec.length; i++ ) {
			final int split = virtualDocumentGapSpec[ i ].indexOf( ':' );
			if ( split >= 0 ) {
				final String fieldName = virtualDocumentGapSpec[ i ].substring( 0, split );
				final int field = factory.fieldIndex( fieldName );
				if ( field < 0 ) throw new IllegalArgumentException( "Field " + fieldName + " is not part of factory " + factory.getClass().getName() );
				if ( !indexedFields.contains( field ) ) throw new IllegalArgumentException( "Field " + factory.fieldName( field ) + " is not being indexed" );
				if ( factory.fieldType( field ) != DocumentFactory.FieldType.VIRTUAL ) throw new IllegalArgumentException( "Field " + factory.fieldName( field ) + " is not virtual" );
				try {
					virtualDocumentGap[ indexedFields.indexOf( field ) ] = Integer.parseInt( virtualDocumentGapSpec[ i ].substring( split + 1 ) );
					if ( virtualDocumentGap[ indexedFields.indexOf( field ) ] < 0 ) throw new NumberFormatException( "Gap can't be negative" );
				}
				catch ( NumberFormatException e ) {
					throw new RuntimeException( "Cannot parse gap correctly " + virtualDocumentGapSpec[ i ], e );
				}
			}
		}
		return virtualDocumentGap;
	}

	public static int[] parseFieldNames( final String[] indexedFieldName, final DocumentFactory factory, final boolean allSupported ) {
		final IntArrayList indexedFields = new IntArrayList();

		if ( indexedFieldName.length == 0 ) {
			for ( int i = 0; i < factory.numberOfFields(); i++ ) {
				DocumentFactory.FieldType type = factory.fieldType( i );
				if ( allSupported ) indexedFields.add( i );
				else if ( type != DocumentFactory.FieldType.VIRTUAL ) indexedFields.add( i );
				else LOGGER.warn( "Virtual field " + factory.fieldName( i ) + " is not being indexed; use -a or explicitly add field among the indexed ones" );
			}
		}
		else {
			for ( int i = 0; i < indexedFieldName.length; i++ ) {
				final int field = factory.fieldIndex( indexedFieldName[ i ] );
				if ( field < 0 ) throw new IllegalArgumentException( "Field " + indexedFieldName[ i ] + " is not part of factory " + factory.getClass().getName() );
				indexedFields.add( field );
			}
		}

		int[] indexedField = indexedFields.toIntArray();
		Arrays.sort( indexedField );
		return indexedField;
	}
	
	public static IOFactory parseIOFactory( final String ioFactorySpec ) throws IllegalArgumentException, IllegalAccessException, ClassNotFoundException, InvocationTargetException, InstantiationException, NoSuchMethodException, IOException {
		try {
			final Field field = IOFactory.class.getField( ioFactorySpec );
			return (IOFactory)field.get( null );
		}
		catch( NoSuchFieldException e ) {}
		
		return (IOFactory)ObjectParser.fromSpec( ioFactorySpec );
	}

	/**
	 * Returns the document sequence to be indexed.
	 * 
	 * @param sequenceName the name of a serialised document sequence, or {@code null} for
	 * standard input.
	 * @param factoryClass the class of the {@link DocumentFactory} that should be passed to the
	 * document sequence.
	 * @param property an array of property strings to be used in the factory initialisation.
	 * @param delimiter a delimiter in case we want to use standard input.
	 * @param logger a logger.
	 * @return the document sequence to be indexed.
	 */
	public static DocumentSequence getSequence( final String sequenceName, final Class<?> factoryClass, final String[] property, final int delimiter, Logger logger ) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException, IOException, ClassNotFoundException, InstantiationException, IllegalArgumentException, SecurityException {
		if ( sequenceName != null ) {
			return AbstractDocumentSequence.load( sequenceName );
		}
		else {
			logger.debug( "Documents will be separated by the Unicode character " + delimiter );
			DocumentFactory factory = PropertyBasedDocumentFactory.getInstance( factoryClass, property );
			return new InputStreamDocumentSequence( System.in, delimiter, factory );
		}
	}

	@SuppressWarnings("unchecked")
	public static void main( final String[] arg ) throws JSAPException, InvocationTargetException, NoSuchMethodException, ConfigurationException, ClassNotFoundException, IOException,
			IllegalAccessException, InstantiationException {

		SimpleJSAP jsap = new SimpleJSAP(
				Scan.class.getName(),
				"Builds a set of batches from a sequence of documents.",
				new Parameter[] {
						new FlaggedOption( "sequence", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "sequence", "A serialised document sequence that will be used instead of stdin." ),
						new FlaggedOption( "ioFactory", JSAP.STRING_PARSER, "FILESYSTEM_FACTORY", JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "io-factory", "An I/O factory that will be used to create files (either a static field of IOFactory or an object specification)." ),
						new FlaggedOption( "objectSequence", new ObjectParser( DocumentSequence.class, MG4JClassParser.PACKAGE ), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "object-sequence", "An object specification describing a document sequence that will be used instead of stdin." ),
						new FlaggedOption( "delimiter", JSAP.INTEGER_PARSER, Integer.toString( DEFAULT_DELIMITER ), JSAP.NOT_REQUIRED, 'd', "delimiter", "The document delimiter (when indexing stdin)." ),
						new FlaggedOption( "factory", MG4JClassParser.getParser(), IdentityDocumentFactory.class.getName(), JSAP.NOT_REQUIRED, 'f', "factory", "A document factory with a standard constructor (when indexing stdin)." ),
						new FlaggedOption( "property", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "property", "A 'key=value' specification, or the name of a property file (when indexing stdin)." )
								.setAllowMultipleDeclarations( true ),
						new FlaggedOption( "termProcessor", JSAP.STRING_PARSER, NullTermProcessor.class.getName(), JSAP.NOT_REQUIRED, 't', "term-processor",
								"Sets the term processor to the given class." ),
						new FlaggedOption( "completeness", JSAP.STRING_PARSER, Completeness.POSITIONS.name(), JSAP.NOT_REQUIRED, 'c', "completeness", "How complete the index should be " + Arrays.toString( Completeness.values() ) + "." ),
						new Switch( "downcase", JSAP.NO_SHORTFLAG, "downcase", "A shortcut for setting the term processor to the downcasing processor." ),
						new FlaggedOption( "indexedField", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'I', "indexed-field",
								"The field(s) of the document factory that will be indexed. (default: all non-virtual fields)" ).setAllowMultipleDeclarations( true ),
						new Switch( "allFields", 'a', "all-fields", "Index also all virtual fields; has no effect if indexedField has been used at least once." ),
						new FlaggedOption( "buildCollection", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'B', "build-collection", "During the indexing phase, build a collection using this basename." ),
						new FlaggedOption( "builderClass", MG4JClassParser.getParser(), SimpleCompressedDocumentCollectionBuilder.class.getName(), JSAP.NOT_REQUIRED, JSAP.NO_SHORTFLAG, "builder-class", "Specifies a builder class for a document collection that will be created during the indexing phase." ),
						new Switch( "exact", 'e', "exact", "The builder class should be instantiated in its exact form, which records both words and nonwords." ),
						new FlaggedOption( "batchSize", JSAP.INTSIZE_PARSER, Integer.toString( Scan.DEFAULT_BATCH_SIZE ), JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in documents. Batches will be smaller, however, if memory is exhausted or there are too many terms." ),
						new FlaggedOption( "maxTerms", JSAP.INTSIZE_PARSER, Integer.toString( Scan.DEFAULT_MAX_TERMS ), JSAP.NOT_REQUIRED, 'M', "max-terms", "The maximum number of terms in a batch, in documents." ),
						new FlaggedOption( "virtualDocumentResolver", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'v', "virtual-document-resolver",
								"The virtual document resolver. It can be specified several times in the form [<field>:]<filename>. If the field is omitted, it sets the document resolver for all virtual fields." )
								.setAllowMultipleDeclarations( true ),
						new FlaggedOption( "virtualDocumentGap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'g', "virtual-document-gap",
								"The virtual document gap. It can be specified several times in the form [<field>:]<gap>. If the field is omitted, it sets the document gap for all virtual fields; the default gap is "
										+ DEFAULT_VIRTUAL_DOCUMENT_GAP ).setAllowMultipleDeclarations( true ),
						new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer." ),
						new FlaggedOption( "renumber", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "renumber", "The filename of a document renumbering." ),
						new Switch( "keepUnsorted", 'u', "keep-unsorted", "Keep the unsorted term file." ),
						new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval",
								"The minimum time interval between activity logs in milliseconds." ),
						new FlaggedOption( "tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "temp-dir", "A directory for all temporary files (e.g., batches)." ),
						new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the resulting index." ) } );

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) return;

		if ( ( jsapResult.userSpecified( "builderClass" ) || jsapResult.userSpecified( "exact" ) ) && ! jsapResult.userSpecified( "buildCollection" ) )	throw new IllegalArgumentException( "To specify options about the collection building process, you must specify a basename first." );
		if ( jsapResult.userSpecified( "sequence" ) && jsapResult.userSpecified( "objectSequence" ) ) throw new IllegalArgumentException( "You cannot specify both a serialised and an parseable-object sequence" );
		
		final DocumentSequence documentSequence = jsapResult.userSpecified( "objectSequence" ) ? (DocumentSequence)jsapResult.getObject( "objectSequence" ) : Scan.getSequence( jsapResult.getString( "sequence" ), jsapResult.getClass( "factory" ), jsapResult.getStringArray( "property" ), jsapResult.getInt( "delimiter" ), LOGGER );

		final IOFactory ioFactory = parseIOFactory( jsapResult.getString( "ioFactory" ) );

		final DocumentFactory factory = documentSequence.factory();
		final int[] indexedField = parseFieldNames( jsapResult.getStringArray( "indexedField" ), factory, jsapResult.getBoolean( "allFields" ) );
		final int batchSize = jsapResult.getInt( "batchSize" );
		final VirtualDocumentResolver[] virtualDocumentResolver = parseVirtualDocumentResolver( ioFactory, jsapResult.getStringArray( "virtualDocumentResolver" ), indexedField, factory );
		final int[] virtualDocumentGap = parseVirtualDocumentGap( jsapResult.getStringArray( "virtualDocumentGap" ), indexedField, factory );

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

		run( ioFactory, jsapResult.getString( "basename" ), documentSequence, Completeness.valueOf( jsapResult.getString( "completeness" ) ), jsapResult.getBoolean( "downcase" ) ? DowncaseTermProcessor.getInstance() : ObjectParser.fromSpec( jsapResult
				.getString( "termProcessor" ), TermProcessor.class, MG4JClassParser.PACKAGE, new String[] { "getInstance" } ), builder, jsapResult
				.getInt( "bufferSize" ), batchSize, jsapResult.getInt( "maxTerms" ), indexedField, virtualDocumentResolver, virtualDocumentGap, jsapResult.getString( "renumber" ), jsapResult.getLong( "logInterval" ), jsapResult
				.getString( "tempDir" ) );
	}
}
