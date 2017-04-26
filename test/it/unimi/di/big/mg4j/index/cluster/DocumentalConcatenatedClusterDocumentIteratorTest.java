package it.unimi.di.big.mg4j.index.cluster;

import static org.junit.Assert.assertEquals;
import it.unimi.di.big.mg4j.document.StringArrayDocumentCollection;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.tool.Combine.IndexType;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.di.big.mg4j.tool.PartitionDocumentally;
import it.unimi.dsi.big.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;

import java.io.File;

import org.junit.Test;

public class DocumentalConcatenatedClusterDocumentIteratorTest {

	@Test
	public void testSkipToBeyondUsedClusters() throws Exception {
		/* We test what happens when we skip to a document belonging to a local index larger 
		 * than any index in which the term appears. */
		
		final String basename = File.createTempFile( getClass().getSimpleName(), "test" ).getCanonicalPath();
        new IndexBuilder( basename, new StringArrayDocumentCollection( "A B", "B", "A", "A" ) ).run();
		BinIO.storeObject( DocumentalStrategies.uniform( 2, 4 ), basename + "-strategy" );
		new PartitionDocumentally( basename + "-text", basename + "-cluster", DocumentalStrategies.uniform( 2, 4 ), basename + "-strategy", 0, 1024, CompressionFlags.DEFAULT_STANDARD_INDEX, IndexType.INTERLEAVED, false, 0, 0, 0, ProgressLogger.DEFAULT_LOG_INTERVAL ).run();
		FileLinesCollection flc;
		flc = new FileLinesCollection( basename + "-cluster-0.terms", "ASCII" );
		BinIO.storeObject( new ShiftAddXorSignedStringMap( flc.iterator(), new GOV3Function.Builder<CharSequence>().keys(flc).transform(TransformationStrategies.utf16()).build()), basename + "-cluster-0.termmap" );  
		flc = new FileLinesCollection( basename + "-cluster-1.terms", "ASCII" );
		BinIO.storeObject( new ShiftAddXorSignedStringMap( flc.iterator(), new GOV3Function.Builder<CharSequence>().keys(flc).transform(TransformationStrategies.utf16()).build()), basename + "-cluster-1.termmap" );  
		Index index = Index.getInstance( basename + "-cluster" );
		assertEquals( DocumentIterator.END_OF_LIST, index.documents( "b" ).skipTo( 2 ) );
	}
	
}
