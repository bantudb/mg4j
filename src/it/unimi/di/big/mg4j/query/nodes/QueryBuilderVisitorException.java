package it.unimi.di.big.mg4j.query.nodes;

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


/** A wrapper for unchecked exceptions thrown during a visit.
 * 
 * <p>Since the operations of a visiting method are generic, any exception might
 * be thrown by such a method. To avoid throwing all kinds of exception, unchecked
 * exception thrown by visiting methods are wrapped by instances of this class
 * (much like exceptions thrown by reflective calls are wrapped by {@link java.lang.reflect.InvocationTargetException}). 
 * 
 * @author Sebastiano Vigna
 */

public class QueryBuilderVisitorException extends Exception {

	private static final long serialVersionUID = 1L;

	public QueryBuilderVisitorException( final Throwable cause ) {
		super( cause );
	}
}
