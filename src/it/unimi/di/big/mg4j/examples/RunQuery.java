package it.unimi.di.big.mg4j.examples;

/*		 
* MG4J: Managing Gigabytes for Java (big)
*
* Copyright (C) 2009-2016 Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.document.HtmlDocumentFactory;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.IntervalSelector;
import it.unimi.di.big.mg4j.query.QueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.BM25Scorer;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;

/** A very simple example that shows how to load a couple of indices and run them using
 * a {@linkplain QueryEngine query engine}. First argument is the basename of an index (possibly produced
 * by an {@link HtmlDocumentFactory}) that has fields <code>title</code> and <code>text</code>.
 * Second argument is a query.
 * 
 * @author Sebastiano Vigna
 * @since 2.2
 */

public class RunQuery {

	public static void main( String arg[] ) throws Exception {
		/** First we open our indices. The booleans tell that we want random access to
		 * the inverted lists, and we are going to use document sizes (for scoring--see below). */
		final Index text = Index.getInstance( arg[ 0 ] + "-text", true, true );
		final Index title = Index.getInstance( arg[ 0 ] + "-title", true, true );

		/* We need a map mapping index names to actual indices. Its keyset will be used by the
		 * parser to distinguish correct index names (e.g., "text:foo title:bar"), and the mapping
		 * itself will be used when transforming a query into a document iterator. We use a handy
		 * fastutil array-based constructor. */
		Object2ReferenceOpenHashMap<String,Index> indexMap = 
			new Object2ReferenceOpenHashMap<String,Index>( new String[] { "text", "title" }, new Index[] { text, title } );
		
		/* We now need to map index names to term processors. This is necessary as any processing
		 * applied during indexing must be applied at query time, too. */
		Object2ReferenceOpenHashMap<String, TermProcessor> termProcessors = 
			new Object2ReferenceOpenHashMap<String,TermProcessor>( new String[] { "text", "title" }, new TermProcessor[] { text.termProcessor, title.termProcessor } );
		
		/* To run a query in a simple way we need a query engine. The engine requires a parser
		 * (which in turn requires the set of index names and a default index), a document iterator
		 * builder, which needs the index map, a default index, and a limit on prefix query
		 * expansion, and finally the index map. */
		QueryEngine engine = new QueryEngine(
			new SimpleParser( indexMap.keySet(), "text", termProcessors ),
			new DocumentIteratorBuilderVisitor( indexMap, text, 1000 ), 
			indexMap
			
		);

		/* Optionally, we can score the results. Here we use a state-of-art ranking 
		 * function, BM25, which requires document sizes. */
		engine.score( new BM25Scorer() );
		
		/* Optionally, we can weight the importance of each index. To do so, we have to pass a map,
		 * and again we use the handy fastutil constructor. Note that setting up a BM25F scorer
		 * would give much better results, but we want to keep it simple. */
		engine.setWeights( new Reference2DoubleOpenHashMap<Index>( new Index[] { text, title }, new double[] { 1, 2 } ) );
		
		/* Optionally, we can use an interval selector to get intervals representing matches. */
		engine.intervalSelector = new IntervalSelector();
		
		/* We are ready to run our query. We just need a list to store its results. The list is made
		 * of DocumentScoreInfo objects, which comprise a document id, a score, and possibly an
		 * info field that is generic. Here the info field is a map from indices to arrays
		 * of selected intervals. This part will be empty if we do not set an interval selector. */
		ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> result = 
			new ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index,SelectedInterval[]>>>();

		/* The query engine can return any subsegment of the results of a query. Here we grab the first 20 results. */
		engine.process( arg[ 1 ], 0, 20, result );
		
		for( DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : result ) {
			System.out.println( dsi.document + " " + dsi.score );
		}
	}
}
