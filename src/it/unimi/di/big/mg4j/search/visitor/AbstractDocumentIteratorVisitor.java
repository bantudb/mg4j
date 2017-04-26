package it.unimi.di.big.mg4j.search.visitor;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2006-2016 Sebastiano Vigna 
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

import it.unimi.di.big.mg4j.index.IndexIterator;
import it.unimi.di.big.mg4j.index.MultiTermIndexIterator;
import it.unimi.di.big.mg4j.search.DocumentIterator;
import it.unimi.di.big.mg4j.search.FalseDocumentIterator;
import it.unimi.di.big.mg4j.search.TrueDocumentIterator;

import java.io.IOException;

/** An abstract implementation of a {@link it.unimi.di.big.mg4j.search.visitor.DocumentIteratorVisitor} without
 * return values.
 * 
 * <p>This implementation is hardwired to {@link Boolean},
 * returns always true on {@link #visitPre(DocumentIterator)},
 * returns {@link Boolean#TRUE} on all internal nodes if no subnode returns <code>null</code>,
 * returns {@link Boolean#TRUE} on {@linkplain TrueDocumentIterator true}/{@linkplain FalseDocumentIterator false} document iterators,
 * returns <code>null</code> on calls to {@link #newArray(int)},
 * and delegates {@link #visit(MultiTermIndexIterator)} to {@link DocumentIteratorVisitor#visit(IndexIterator)}.
 */

public abstract class AbstractDocumentIteratorVisitor implements DocumentIteratorVisitor<Boolean> {
	public AbstractDocumentIteratorVisitor prepare() { return this; }
	
	public boolean visitPre( final DocumentIterator documentIterator ) { return true; }

	public Boolean[] newArray( int len ) { return null; }

	public Boolean visitPost( DocumentIterator documentIterator, Boolean[] subNodeResult ) {
		if ( subNodeResult != null ) for( int i = subNodeResult.length; i-- != 0; ) if ( subNodeResult[ i ] == null ) return null;
		return Boolean.TRUE; 
	}

	public Boolean visit( MultiTermIndexIterator indexIterator ) throws IOException {
		return visit( (IndexIterator)indexIterator );
	}
	
	public Boolean visit( TrueDocumentIterator trueDocumentIterator ) {
		return Boolean.TRUE;
	}

	public Boolean visit( FalseDocumentIterator falseDocumentIterator ) {
		return Boolean.TRUE;
	}
}
