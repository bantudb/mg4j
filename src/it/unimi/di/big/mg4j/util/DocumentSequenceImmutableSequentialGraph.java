package it.unimi.di.big.mg4j.util;

/*		 
 * MG4J: Managing Gigabytes for Java
 *
 * Copyright (C) 2007-2016 Paolo Boldi and Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.tool.Scan;
import it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment;
import it.unimi.di.big.mg4j.tool.VirtualDocumentResolver;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.lang.ObjectParser;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.NoSuchElementException;

/** Exposes a document sequence as a (sequentially accessible) immutable graph, according to some
 *  virtual field provided by the documents in the sequence. A suitable {@link VirtualDocumentResolver}
 *  is used to associate node numbers to each fragment.
 *  
 *  <p>More precisely, the graph will have as many nodes as there are documents in the sequence, the
 *  <var>k</var>-th document (starting from 0) representing node number <var>k</var>.
 *  The successors of a document are obtained by extracting the virtual field from the
 *  document, turning each {@linkplain it.unimi.di.big.mg4j.tool.Scan.VirtualDocumentFragment document specifier}
 *  into a document number (using the given {@linkplain VirtualDocumentResolver resolver},
 *  and discarding unresolved URLs).
 *  
 *  <p>Note that the current implementation does not support more than {@link Integer#MAX_VALUE} successors per node.
 */
public class DocumentSequenceImmutableSequentialGraph extends ImmutableSequentialGraph {
	/** The underlying sequence. */
	private DocumentSequence sequence;
	/** The number of the virtual field to be used. */
	private int virtualField;
	/** The resolver to be used. */
	private VirtualDocumentResolver resolver;
	/** The number of nodes (set by the iterator at the end of iteration). */
	private long numNodes;
	
	/** Creates an immutable graph from a sequence.
	 * 
	 * @param sequence the sequence whence the immutable graph should be created.
	 * @param fieldName the name of the virtual field to be used to get the successors.
	 * @param resolver the resolver to be used to map document specs to node numbers.
	 */
	public DocumentSequenceImmutableSequentialGraph( final DocumentSequence sequence, final String fieldName, final VirtualDocumentResolver resolver ) {
		this.sequence = sequence;
		this.virtualField = sequence.factory().fieldIndex( fieldName );
		this.resolver = resolver;
		this.numNodes = -1;
	}

	/** Creates a new immutable graph with the specified arguments.
	 * 
	 * @param arg a 3-element array: the first is a {@link ObjectParser specification} of a {@link DocumentSequence}, 
	 * the second is the name of a virtual field, and the third is the filename of a {@link VirtualDocumentResolver}.
	 */
	public DocumentSequenceImmutableSequentialGraph( final String... arg ) throws IOException, ClassNotFoundException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
		this( ObjectParser.fromSpec( arg[ 0 ], DocumentSequence.class ), arg[ 1 ], (VirtualDocumentResolver)BinIO.loadObject( arg[ 2 ] ) );
	}
	
	@Override
	public ImmutableGraph copy() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long numNodes() {
		return numNodes; // This must not be called before the end of the first iteration.
	}

	@Override
	public boolean randomAccess() {
		return false;
	}
	
	public NodeIterator nodeIterator() {
		try {
			final DocumentIterator documentIterator = sequence.iterator();
			return new NodeIterator() {
				private Document cachedDocument = documentIterator.nextDocument();
				private long cachedDocumentNumber = 0;
				private long[] cachedSuccessors;
				private LongOpenHashSet succ = new LongOpenHashSet();

				public boolean hasNext() {
					if ( cachedDocument != null ) return true;
					numNodes = cachedDocumentNumber;
					return false;
				}
				
				@SuppressWarnings("unchecked")
				public long nextLong() {
					if ( !hasNext() ) throw new NoSuchElementException();
					ObjectList<Scan.VirtualDocumentFragment> vdfList;
					try {
						vdfList = (ObjectList<VirtualDocumentFragment>)cachedDocument.content( virtualField );
					}
					catch ( IOException e ) {
						throw new RuntimeException( e.getMessage(), e );
					}
					succ.clear();
					resolver.context( cachedDocument );
					ObjectIterator<VirtualDocumentFragment> it = vdfList.iterator();
					while ( it.hasNext() ) {
						final long successor = resolver.resolve( it.next().documentSpecifier() );
						if ( successor >= 0 ) succ.add( successor );
					}
					cachedSuccessors = succ.toLongArray();
					Arrays.sort( cachedSuccessors );
					// Get ready for the next request
					try {
						cachedDocument.close();
						cachedDocument = documentIterator.nextDocument();
					}
					catch ( IOException e ) {
						throw new RuntimeException( e );
					}
					return cachedDocumentNumber++; 
				}

				public long outdegree() {
					return cachedSuccessors.length;
				}
				
				@Override
				public long[][] successorBigArray() {
					return LongBigArrays.wrap( cachedSuccessors );
				}				
			};
		}
		catch ( IOException e ) {
			throw new RuntimeException( e );
		}
	}
}
