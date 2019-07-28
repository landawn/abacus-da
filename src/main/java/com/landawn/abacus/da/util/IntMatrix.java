/*
 * Copyright (C) 2016 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.da.util;

import java.util.NoSuchElementException;

import com.landawn.abacus.annotation.Beta;
import com.landawn.abacus.util.Array;
import com.landawn.abacus.util.IntList;
import com.landawn.abacus.util.IntPair;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Try;
import com.landawn.abacus.util.Try.Consumer;
import com.landawn.abacus.util.f;
import com.landawn.abacus.util.u.OptionalInt;
import com.landawn.abacus.util.function.IntConsumer;
import com.landawn.abacus.util.stream.IntIteratorEx;
import com.landawn.abacus.util.stream.IntStream;
import com.landawn.abacus.util.stream.ObjIteratorEx;
import com.landawn.abacus.util.stream.Stream;

// TODO: Auto-generated Javadoc
/**
 * The Class IntMatrix.
 *
 * @author Haiyang Li
 * @since 0.8
 */
public final class IntMatrix extends AbstractMatrix<int[], IntList, IntStream, Stream<IntStream>, IntMatrix> {

    /** The Constant EMPTY_INT_MATRIX. */
    static final IntMatrix EMPTY_INT_MATRIX = new IntMatrix(new int[0][0]);

    /**
     * Instantiates a new int matrix.
     *
     * @param a the a
     */
    public IntMatrix(final int[][] a) {
        super(a == null ? new int[0][0] : a);
    }

    /**
     * Empty.
     *
     * @return the int matrix
     */
    public static IntMatrix empty() {
        return EMPTY_INT_MATRIX;
    }

    /**
     * Of.
     *
     * @param a the a
     * @return the int matrix
     */
    @SafeVarargs
    public static IntMatrix of(final int[]... a) {
        return N.isNullOrEmpty(a) ? EMPTY_INT_MATRIX : new IntMatrix(a);
    }

    /**
     * From.
     *
     * @param a the a
     * @return the int matrix
     */
    @SafeVarargs
    public static IntMatrix from(final char[]... a) {
        if (N.isNullOrEmpty(a)) {
            return EMPTY_INT_MATRIX;
        }

        final int[][] c = new int[a.length][a[0].length];

        for (int i = 0, len = a.length; i < len; i++) {
            for (int j = 0, col = a[0].length; j < col; j++) {
                c[i][j] = a[i][j];
            }
        }

        return new IntMatrix(c);
    }

    /**
     * From.
     *
     * @param a the a
     * @return the int matrix
     */
    @SafeVarargs
    public static IntMatrix from(final byte[]... a) {
        if (N.isNullOrEmpty(a)) {
            return EMPTY_INT_MATRIX;
        }

        final int[][] c = new int[a.length][a[0].length];

        for (int i = 0, len = a.length; i < len; i++) {
            for (int j = 0, col = a[0].length; j < col; j++) {
                c[i][j] = a[i][j];
            }
        }

        return new IntMatrix(c);
    }

    /**
     * From.
     *
     * @param a the a
     * @return the int matrix
     */
    @SafeVarargs
    public static IntMatrix from(final short[]... a) {
        if (N.isNullOrEmpty(a)) {
            return EMPTY_INT_MATRIX;
        }

        final int[][] c = new int[a.length][a[0].length];

        for (int i = 0, len = a.length; i < len; i++) {
            for (int j = 0, col = a[0].length; j < col; j++) {
                c[i][j] = a[i][j];
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Random.
     *
     * @param len the len
     * @return the int matrix
     */
    public static IntMatrix random(final int len) {
        return new IntMatrix(new int[][] { IntList.random(len).array() });
    }

    /**
     * Repeat.
     *
     * @param val the val
     * @param len the len
     * @return the int matrix
     */
    public static IntMatrix repeat(final int val, final int len) {
        return new IntMatrix(new int[][] { Array.repeat(val, len) });
    }

    /**
     * Range.
     *
     * @param startInclusive the start inclusive
     * @param endExclusive the end exclusive
     * @return the int matrix
     */
    public static IntMatrix range(int startInclusive, final int endExclusive) {
        return new IntMatrix(new int[][] { Array.range(startInclusive, endExclusive) });
    }

    /**
     * Range.
     *
     * @param startInclusive the start inclusive
     * @param endExclusive the end exclusive
     * @param by the by
     * @return the int matrix
     */
    public static IntMatrix range(int startInclusive, final int endExclusive, final int by) {
        return new IntMatrix(new int[][] { Array.range(startInclusive, endExclusive, by) });
    }

    /**
     * Range closed.
     *
     * @param startInclusive the start inclusive
     * @param endInclusive the end inclusive
     * @return the int matrix
     */
    public static IntMatrix rangeClosed(int startInclusive, final int endInclusive) {
        return new IntMatrix(new int[][] { Array.rangeClosed(startInclusive, endInclusive) });
    }

    /**
     * Range closed.
     *
     * @param startInclusive the start inclusive
     * @param endInclusive the end inclusive
     * @param by the by
     * @return the int matrix
     */
    public static IntMatrix rangeClosed(int startInclusive, final int endInclusive, final int by) {
        return new IntMatrix(new int[][] { Array.rangeClosed(startInclusive, endInclusive, by) });
    }

    /**
     * Diagonal LU 2 RD.
     *
     * @param leftUp2RighDownDiagonal the left up 2 righ down diagonal
     * @return the int matrix
     */
    public static IntMatrix diagonalLU2RD(final int[] leftUp2RighDownDiagonal) {
        return diagonal(leftUp2RighDownDiagonal, null);
    }

    /**
     * Diagonal RU 2 LD.
     *
     * @param rightUp2LeftDownDiagonal the right up 2 left down diagonal
     * @return the int matrix
     */
    public static IntMatrix diagonalRU2LD(final int[] rightUp2LeftDownDiagonal) {
        return diagonal(null, rightUp2LeftDownDiagonal);
    }

    /**
     * Diagonal.
     *
     * @param leftUp2RighDownDiagonal the left up 2 righ down diagonal
     * @param rightUp2LeftDownDiagonal the right up 2 left down diagonal
     * @return the int matrix
     */
    public static IntMatrix diagonal(final int[] leftUp2RighDownDiagonal, int[] rightUp2LeftDownDiagonal) {
        N.checkArgument(
                N.isNullOrEmpty(leftUp2RighDownDiagonal) || N.isNullOrEmpty(rightUp2LeftDownDiagonal)
                        || leftUp2RighDownDiagonal.length == rightUp2LeftDownDiagonal.length,
                "The length of 'leftUp2RighDownDiagonal' and 'rightUp2LeftDownDiagonal' must be same");

        if (N.isNullOrEmpty(leftUp2RighDownDiagonal)) {
            if (N.isNullOrEmpty(rightUp2LeftDownDiagonal)) {
                return empty();
            } else {
                final int len = rightUp2LeftDownDiagonal.length;
                final int[][] c = new int[len][len];

                for (int i = 0, j = len - 1; i < len; i++, j--) {
                    c[i][j] = rightUp2LeftDownDiagonal[i];
                }

                return new IntMatrix(c);
            }
        } else {
            final int len = leftUp2RighDownDiagonal.length;
            final int[][] c = new int[len][len];

            for (int i = 0; i < len; i++) {
                c[i][i] = leftUp2RighDownDiagonal[i];
            }

            if (N.notNullOrEmpty(rightUp2LeftDownDiagonal)) {
                for (int i = 0, j = len - 1; i < len; i++, j--) {
                    c[i][j] = rightUp2LeftDownDiagonal[i];
                }
            }

            return new IntMatrix(c);
        }
    }

    /**
     * Gets the.
     *
     * @param i the i
     * @param j the j
     * @return the int
     */
    public int get(final int i, final int j) {
        return a[i][j];
    }

    /**
     * Gets the.
     *
     * @param point the point
     * @return the int
     */
    public int get(final IntPair point) {
        return a[point._1][point._2];
    }

    /**
     * Sets the.
     *
     * @param i the i
     * @param j the j
     * @param val the val
     */
    public void set(final int i, final int j, final int val) {
        a[i][j] = val;
    }

    /**
     * Sets the.
     *
     * @param point the point
     * @param val the val
     */
    public void set(final IntPair point, final int val) {
        a[point._1][point._2] = val;
    }

    /**
     * Up of.
     *
     * @param i the i
     * @param j the j
     * @return the optional int
     */
    public OptionalInt upOf(final int i, final int j) {
        return i == 0 ? OptionalInt.empty() : OptionalInt.of(a[i - 1][j]);
    }

    /**
     * Down of.
     *
     * @param i the i
     * @param j the j
     * @return the optional int
     */
    public OptionalInt downOf(final int i, final int j) {
        return i == rows - 1 ? OptionalInt.empty() : OptionalInt.of(a[i + 1][j]);
    }

    /**
     * Left of.
     *
     * @param i the i
     * @param j the j
     * @return the optional int
     */
    public OptionalInt leftOf(final int i, final int j) {
        return j == 0 ? OptionalInt.empty() : OptionalInt.of(a[i][j - 1]);
    }

    /**
     * Right of.
     *
     * @param i the i
     * @param j the j
     * @return the optional int
     */
    public OptionalInt rightOf(final int i, final int j) {
        return j == cols - 1 ? OptionalInt.empty() : OptionalInt.of(a[i][j + 1]);
    }

    /**
     * Returns the four adjacencies with order: up, right, down, left. <code>null</code> is set if the adjacency doesn't exist.
     *
     * @param i the i
     * @param j the j
     * @return the stream
     */
    public Stream<IntPair> adjacent4Points(final int i, final int j) {
        final IntPair up = i == 0 ? null : IntPair.of(i - 1, j);
        final IntPair right = j == cols - 1 ? null : IntPair.of(i, j + 1);
        final IntPair down = i == rows - 1 ? null : IntPair.of(i + 1, j);
        final IntPair left = j == 0 ? null : IntPair.of(i, j - 1);

        return Stream.of(up, right, down, left);
    }

    /**
     * Returns the eight adjacencies with order: left-up, up, right-up, right, right-down, down, left-down, left. <code>null</code> is set if the adjacency doesn't exist.
     *
     * @param i the i
     * @param j the j
     * @return the stream
     */
    public Stream<IntPair> adjacent8Points(final int i, final int j) {
        final IntPair up = i == 0 ? null : IntPair.of(i - 1, j);
        final IntPair right = j == cols - 1 ? null : IntPair.of(i, j + 1);
        final IntPair down = i == rows - 1 ? null : IntPair.of(i + 1, j);
        final IntPair left = j == 0 ? null : IntPair.of(i, j - 1);

        final IntPair leftUp = i > 0 && j > 0 ? IntPair.of(i - 1, j - 1) : null;
        final IntPair rightUp = i > 0 && j < cols - 1 ? IntPair.of(i - 1, j + 1) : null;
        final IntPair rightDown = i < rows - 1 && j < cols - 1 ? IntPair.of(j + 1, j + 1) : null;
        final IntPair leftDown = i < rows - 1 && j > 0 ? IntPair.of(i + 1, j - 1) : null;

        return Stream.of(leftUp, up, rightUp, right, rightDown, down, leftDown, left);
    }

    /**
     * Row.
     *
     * @param rowIndex the row index
     * @return the int[]
     */
    public int[] row(final int rowIndex) {
        N.checkArgument(rowIndex >= 0 && rowIndex < rows, "Invalid row Index: %s", rowIndex);

        return a[rowIndex];
    }

    /**
     * Column.
     *
     * @param columnIndex the column index
     * @return the int[]
     */
    public int[] column(final int columnIndex) {
        N.checkArgument(columnIndex >= 0 && columnIndex < cols, "Invalid column Index: %s", columnIndex);

        final int[] c = new int[rows];

        for (int i = 0; i < rows; i++) {
            c[i] = a[i][columnIndex];
        }

        return c;
    }

    /**
     * Sets the row.
     *
     * @param rowIndex the row index
     * @param row the row
     */
    public void setRow(int rowIndex, int[] row) {
        N.checkArgument(row.length == cols, "The size of the specified row doesn't match the length of column");

        N.copy(row, 0, a[rowIndex], 0, cols);
    }

    /**
     * Sets the column.
     *
     * @param columnIndex the column index
     * @param column the column
     */
    public void setColumn(int columnIndex, int[] column) {
        N.checkArgument(column.length == rows, "The size of the specified column doesn't match the length of row");

        for (int i = 0; i < rows; i++) {
            a[i][columnIndex] = column[i];
        }
    }

    /**
     * Update row.
     *
     * @param <E> the element type
     * @param rowIndex the row index
     * @param func the func
     * @throws E the e
     */
    public <E extends Exception> void updateRow(int rowIndex, Try.IntUnaryOperator<E> func) throws E {
        for (int i = 0; i < cols; i++) {
            a[rowIndex][i] = func.applyAsInt(a[rowIndex][i]);
        }
    }

    /**
     * Update column.
     *
     * @param <E> the element type
     * @param columnIndex the column index
     * @param func the func
     * @throws E the e
     */
    public <E extends Exception> void updateColumn(int columnIndex, Try.IntUnaryOperator<E> func) throws E {
        for (int i = 0; i < rows; i++) {
            a[i][columnIndex] = func.applyAsInt(a[i][columnIndex]);
        }
    }

    /**
     * Gets the lu2rd.
     *
     * @return the lu2rd
     */
    public int[] getLU2RD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        final int[] res = new int[rows];

        for (int i = 0; i < rows; i++) {
            res[i] = a[i][i];
        }

        return res;
    }

    /**
     * Sets the lu2rd.
     *
     * @param diagonal the new lu2rd
     */
    public void setLU2RD(final int[] diagonal) {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);
        N.checkArgument(diagonal.length >= rows, "The length of specified array is less than rows=%s", rows);

        for (int i = 0; i < rows; i++) {
            a[i][i] = diagonal[i];
        }
    }

    /**
     * Update LU 2 RD.
     *
     * @param <E> the element type
     * @param func the func
     * @throws E the e
     */
    public <E extends Exception> void updateLU2RD(final Try.IntUnaryOperator<E> func) throws E {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        for (int i = 0; i < rows; i++) {
            a[i][i] = func.applyAsInt(a[i][i]);
        }
    }

    /**
     * Gets the ru2ld.
     *
     * @return the ru2ld
     */
    public int[] getRU2LD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        final int[] res = new int[rows];

        for (int i = 0; i < rows; i++) {
            res[i] = a[i][cols - i - 1];
        }

        return res;
    }

    /**
     * Sets the ru2ld.
     *
     * @param diagonal the new ru2ld
     */
    public void setRU2LD(final int[] diagonal) {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);
        N.checkArgument(diagonal.length >= rows, "The length of specified array is less than rows=%s", rows);

        for (int i = 0; i < rows; i++) {
            a[i][cols - i - 1] = diagonal[i];
        }
    }

    /**
     * Update RU 2 LD.
     *
     * @param <E> the element type
     * @param func the func
     * @throws E the e
     */
    public <E extends Exception> void updateRU2LD(final Try.IntUnaryOperator<E> func) throws E {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        for (int i = 0; i < rows; i++) {
            a[i][cols - i - 1] = func.applyAsInt(a[i][cols - i - 1]);
        }
    }

    /**
     * Update all.
     *
     * @param <E> the element type
     * @param func the func
     * @throws E the e
     */
    public <E extends Exception> void updateAll(final Try.IntUnaryOperator<E> func) throws E {
        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            a[i][j] = func.applyAsInt(a[i][j]);
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            a[i][j] = func.applyAsInt(a[i][j]);
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        a[i][j] = func.applyAsInt(a[i][j]);
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        a[i][j] = func.applyAsInt(a[i][j]);
                    }
                }
            }
        }
    }

    /**
     * Update all elements based on points.
     *
     * @param <E> the element type
     * @param func the func
     * @throws E the e
     */
    public <E extends Exception> void updateAll(final Try.IntBiFunction<Integer, E> func) throws E {
        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            a[i][j] = func.apply(i, j);
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            a[i][j] = func.apply(i, j);
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        a[i][j] = func.apply(i, j);
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        a[i][j] = func.apply(i, j);
                    }
                }
            }
        }
    }

    /**
     * Replace if.
     *
     * @param <E> the element type
     * @param predicate the predicate
     * @param newValue the new value
     * @throws E the e
     */
    public <E extends Exception> void replaceIf(final Try.IntPredicate<E> predicate, final int newValue) throws E {
        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            a[i][j] = predicate.test(a[i][j]) ? newValue : a[i][j];
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            a[i][j] = predicate.test(a[i][j]) ? newValue : a[i][j];
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        a[i][j] = predicate.test(a[i][j]) ? newValue : a[i][j];
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        a[i][j] = predicate.test(a[i][j]) ? newValue : a[i][j];
                    }
                }
            }
        }
    }

    /**
     * Replace elements by <code>Predicate.test(i, j)</code> based on points
     *
     * @param <E> the element type
     * @param predicate the predicate
     * @param newValue the new value
     * @throws E the e
     */
    public <E extends Exception> void replaceIf(final Try.IntBiPredicate<E> predicate, final int newValue) throws E {
        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            a[i][j] = predicate.test(i, j) ? newValue : a[i][j];
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            a[i][j] = predicate.test(i, j) ? newValue : a[i][j];
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        a[i][j] = predicate.test(i, j) ? newValue : a[i][j];
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        a[i][j] = predicate.test(i, j) ? newValue : a[i][j];
                    }
                }
            }
        }
    }

    /**
     * Map.
     *
     * @param <E> the element type
     * @param func the func
     * @return the int matrix
     * @throws E the e
     */
    public <E extends Exception> IntMatrix map(final Try.IntUnaryOperator<E> func) throws E {
        final int[][] c = new int[rows][cols];

        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            c[i][j] = func.applyAsInt(a[i][j]);
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            c[i][j] = func.applyAsInt(a[i][j]);
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        c[i][j] = func.applyAsInt(a[i][j]);
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        c[i][j] = func.applyAsInt(a[i][j]);
                    }
                }
            }
        }

        return IntMatrix.of(c);
    }

    /**
     * Map to obj.
     *
     * @param <T> the generic type
     * @param <E> the element type
     * @param cls the cls
     * @param func the func
     * @return the matrix
     * @throws E the e
     */
    public <T, E extends Exception> Matrix<T> mapToObj(final Class<T> cls, final Try.IntFunction<? extends T, E> func) throws E {
        final T[][] c = N.newArray(N.newArray(cls, 0).getClass(), rows);

        for (int i = 0; i < rows; i++) {
            c[i] = N.newArray(cls, cols);
        }

        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            c[i][j] = func.apply(a[i][j]);
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            c[i][j] = func.apply(a[i][j]);
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        c[i][j] = func.apply(a[i][j]);
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        c[i][j] = func.apply(a[i][j]);
                    }
                }
            }
        }

        return Matrix.of(c);
    }

    /**
     * Fill.
     *
     * @param val the val
     */
    public void fill(final int val) {
        for (int i = 0; i < rows; i++) {
            N.fill(a[i], val);
        }
    }

    /**
     * Fill.
     *
     * @param b the b
     */
    public void fill(final int[][] b) {
        fill(0, 0, b);
    }

    /**
     * Fill.
     *
     * @param fromRowIndex the from row index
     * @param fromColumnIndex the from column index
     * @param b the b
     */
    public void fill(final int fromRowIndex, final int fromColumnIndex, final int[][] b) {
        N.checkFromToIndex(fromRowIndex, rows, rows);
        N.checkFromToIndex(fromColumnIndex, cols, cols);

        for (int i = 0, minLen = N.min(rows - fromRowIndex, b.length); i < minLen; i++) {
            N.copy(b[i], 0, a[i + fromRowIndex], fromColumnIndex, N.min(b[i].length, cols - fromColumnIndex));
        }
    }

    /**
     * Copy.
     *
     * @return the int matrix
     */
    @Override
    public IntMatrix copy() {
        final int[][] c = new int[rows][];

        for (int i = 0; i < rows; i++) {
            c[i] = a[i].clone();
        }

        return new IntMatrix(c);
    }

    /**
     * Copy.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @return the int matrix
     */
    @Override
    public IntMatrix copy(final int fromRowIndex, final int toRowIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);

        final int[][] c = new int[toRowIndex - fromRowIndex][];

        for (int i = fromRowIndex; i < toRowIndex; i++) {
            c[i - fromRowIndex] = a[i].clone();
        }

        return new IntMatrix(c);
    }

    /**
     * Copy.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @param fromColumnIndex the from column index
     * @param toColumnIndex the to column index
     * @return the int matrix
     */
    @Override
    public IntMatrix copy(final int fromRowIndex, final int toRowIndex, final int fromColumnIndex, final int toColumnIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);
        N.checkFromToIndex(fromColumnIndex, toColumnIndex, cols);

        final int[][] c = new int[toRowIndex - fromRowIndex][];

        for (int i = fromRowIndex; i < toRowIndex; i++) {
            c[i - fromRowIndex] = N.copyOfRange(a[i], fromColumnIndex, toColumnIndex);
        }

        return new IntMatrix(c);
    }

    /**
     * Extend.
     *
     * @param newRows the new rows
     * @param newCols the new cols
     * @return the int matrix
     */
    public IntMatrix extend(final int newRows, final int newCols) {
        return extend(newRows, newCols, 0);
    }

    /**
     * Extend.
     *
     * @param newRows the new rows
     * @param newCols the new cols
     * @param defaultValueForNewCell the default value for new cell
     * @return the int matrix
     */
    public IntMatrix extend(final int newRows, final int newCols, final int defaultValueForNewCell) {
        N.checkArgument(newRows >= 0, "The 'newRows' can't be negative %s", newRows);
        N.checkArgument(newCols >= 0, "The 'newCols' can't be negative %s", newCols);

        if (newRows <= rows && newCols <= cols) {
            return copy(0, newRows, 0, newCols);
        } else {
            final boolean fillDefaultValue = defaultValueForNewCell != 0;
            final int[][] b = new int[newRows][];

            for (int i = 0; i < newRows; i++) {
                b[i] = i < rows ? N.copyOf(a[i], newCols) : new int[newCols];

                if (fillDefaultValue) {
                    if (i >= rows) {
                        N.fill(b[i], defaultValueForNewCell);
                    } else if (cols < newCols) {
                        N.fill(b[i], cols, newCols, defaultValueForNewCell);
                    }
                }
            }

            return new IntMatrix(b);
        }
    }

    /**
     * Extend.
     *
     * @param toUp the to up
     * @param toDown the to down
     * @param toLeft the to left
     * @param toRight the to right
     * @return the int matrix
     */
    public IntMatrix extend(final int toUp, final int toDown, final int toLeft, final int toRight) {
        return extend(toUp, toDown, toLeft, toRight, 0);
    }

    /**
     * Extend.
     *
     * @param toUp the to up
     * @param toDown the to down
     * @param toLeft the to left
     * @param toRight the to right
     * @param defaultValueForNewCell the default value for new cell
     * @return the int matrix
     */
    public IntMatrix extend(final int toUp, final int toDown, final int toLeft, final int toRight, final int defaultValueForNewCell) {
        N.checkArgument(toUp >= 0, "The 'toUp' can't be negative %s", toUp);
        N.checkArgument(toDown >= 0, "The 'toDown' can't be negative %s", toDown);
        N.checkArgument(toLeft >= 0, "The 'toLeft' can't be negative %s", toLeft);
        N.checkArgument(toRight >= 0, "The 'toRight' can't be negative %s", toRight);

        if (toUp == 0 && toDown == 0 && toLeft == 0 && toRight == 0) {
            return copy();
        } else {
            final int newRows = toUp + rows + toDown;
            final int newCols = toLeft + cols + toRight;
            final boolean fillDefaultValue = defaultValueForNewCell != 0;
            final int[][] b = new int[newRows][newCols];

            for (int i = 0; i < newRows; i++) {
                if (i >= toUp && i < toUp + rows) {
                    N.copy(a[i - toUp], 0, b[i], toLeft, cols);
                }

                if (fillDefaultValue) {
                    if (i < toUp || i >= toUp + rows) {
                        N.fill(b[i], defaultValueForNewCell);
                    } else if (cols < newCols) {
                        if (toLeft > 0) {
                            N.fill(b[i], 0, toLeft, defaultValueForNewCell);
                        }

                        if (toRight > 0) {
                            N.fill(b[i], cols + toLeft, newCols, defaultValueForNewCell);
                        }
                    }
                }
            }

            return new IntMatrix(b);
        }
    }

    /**
     * Reverse H.
     */
    public void reverseH() {
        for (int i = 0; i < rows; i++) {
            N.reverse(a[i]);
        }
    }

    /**
     * Reverse V.
     */
    public void reverseV() {
        for (int j = 0; j < cols; j++) {
            int tmp = 0;
            for (int l = 0, h = rows - 1; l < h;) {
                tmp = a[l][j];
                a[l++][j] = a[h][j];
                a[h--][j] = tmp;
            }
        }
    }

    /**
     * Flip H.
     *
     * @return the int matrix
     * @see <a href="https://www.mathworks.com/help/matlab/ref/flip.html#btz149s-1">https://www.mathworks.com/help/matlab/ref/flip.html#btz149s-1</a>
     */
    public IntMatrix flipH() {
        final IntMatrix res = this.copy();
        res.reverseH();
        return res;
    }

    /**
     * Flip V.
     *
     * @return the int matrix
     * @see <a href="https://www.mathworks.com/help/matlab/ref/flip.html#btz149s-1">https://www.mathworks.com/help/matlab/ref/flip.html#btz149s-1</a>
     */
    public IntMatrix flipV() {
        final IntMatrix res = this.copy();
        res.reverseV();
        return res;
    }

    /**
     * Rotate 90.
     *
     * @return the int matrix
     */
    @Override
    public IntMatrix rotate90() {
        final int[][] c = new int[cols][rows];

        if (rows <= cols) {
            for (int j = 0; j < rows; j++) {
                for (int i = 0; i < cols; i++) {
                    c[i][j] = a[rows - j - 1][i];
                }
            }
        } else {
            for (int i = 0; i < cols; i++) {
                for (int j = 0; j < rows; j++) {
                    c[i][j] = a[rows - j - 1][i];
                }
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Rotate 180.
     *
     * @return the int matrix
     */
    @Override
    public IntMatrix rotate180() {
        final int[][] c = new int[rows][];

        for (int i = 0; i < rows; i++) {
            c[i] = a[rows - i - 1].clone();
            N.reverse(c[i]);
        }

        return new IntMatrix(c);
    }

    /**
     * Rotate 270.
     *
     * @return the int matrix
     */
    @Override
    public IntMatrix rotate270() {
        final int[][] c = new int[cols][rows];

        if (rows <= cols) {
            for (int j = 0; j < rows; j++) {
                for (int i = 0; i < cols; i++) {
                    c[i][j] = a[j][cols - i - 1];
                }
            }
        } else {
            for (int i = 0; i < cols; i++) {
                for (int j = 0; j < rows; j++) {
                    c[i][j] = a[j][cols - i - 1];
                }
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Transpose.
     *
     * @return the int matrix
     */
    @Override
    public IntMatrix transpose() {
        final int[][] c = new int[cols][rows];

        if (rows <= cols) {
            for (int j = 0; j < rows; j++) {
                for (int i = 0; i < cols; i++) {
                    c[i][j] = a[j][i];
                }
            }
        } else {
            for (int i = 0; i < cols; i++) {
                for (int j = 0; j < rows; j++) {
                    c[i][j] = a[j][i];
                }
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Reshape.
     *
     * @param newRows the new rows
     * @param newCols the new cols
     * @return the int matrix
     */
    @Override
    public IntMatrix reshape(final int newRows, final int newCols) {
        final int[][] c = new int[newRows][newCols];

        if (newRows == 0 || newCols == 0 || N.isNullOrEmpty(a)) {
            return new IntMatrix(c);
        }

        if (a.length == 1) {
            final int[] a0 = a[0];

            for (int i = 0, len = (int) N.min(newRows, count % newCols == 0 ? count / newCols : count / newCols + 1); i < len; i++) {
                N.copy(a0, i * newCols, c[i], 0, (int) N.min(newCols, count - i * newCols));
            }
        } else {
            long cnt = 0;

            for (int i = 0, len = (int) N.min(newRows, count % newCols == 0 ? count / newCols : count / newCols + 1); i < len; i++) {
                for (int j = 0, col = (int) N.min(newCols, count - i * newCols); j < col; j++, cnt++) {
                    c[i][j] = a[(int) (cnt / this.cols)][(int) (cnt % this.cols)];
                }
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Repeat elements <code>rowRepeats</code> times in row direction and <code>colRepeats</code> times in column direction.
     *
     * @param rowRepeats the row repeats
     * @param colRepeats the col repeats
     * @return a new matrix
     * @see <a href="https://www.mathworks.com/help/matlab/ref/repelem.html">https://www.mathworks.com/help/matlab/ref/repelem.html</a>
     */
    @Override
    public IntMatrix repelem(final int rowRepeats, final int colRepeats) {
        N.checkArgument(rowRepeats > 0 && colRepeats > 0, "rowRepeats=%s and colRepeats=%s must be bigger than 0", rowRepeats, colRepeats);

        final int[][] c = new int[rows * rowRepeats][cols * colRepeats];

        for (int i = 0; i < rows; i++) {
            final int[] fr = c[i * rowRepeats];

            for (int j = 0; j < cols; j++) {
                N.copy(Array.repeat(a[i][j], colRepeats), 0, fr, j * colRepeats, colRepeats);
            }

            for (int k = 1; k < rowRepeats; k++) {
                N.copy(fr, 0, c[i * rowRepeats + k], 0, fr.length);
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Repeat this matrix <code>rowRepeats</code> times in row direction and <code>colRepeats</code> times in column direction.
     *
     * @param rowRepeats the row repeats
     * @param colRepeats the col repeats
     * @return a new matrix
     * @see <a href="https://www.mathworks.com/help/matlab/ref/repmat.html">https://www.mathworks.com/help/matlab/ref/repmat.html</a>
     */
    @Override
    public IntMatrix repmat(final int rowRepeats, final int colRepeats) {
        N.checkArgument(rowRepeats > 0 && colRepeats > 0, "rowRepeats=%s and colRepeats=%s must be bigger than 0", rowRepeats, colRepeats);

        final int[][] c = new int[rows * rowRepeats][cols * colRepeats];

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < colRepeats; j++) {
                N.copy(a[i], 0, c[i], j * cols, cols);
            }
        }

        for (int i = 1; i < rowRepeats; i++) {
            for (int j = 0; j < rows; j++) {
                N.copy(c[j], 0, c[i * rows + j], 0, c[j].length);
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Flatten.
     *
     * @return the int list
     */
    @Override
    public IntList flatten() {
        final int[] c = new int[rows * cols];

        for (int i = 0; i < rows; i++) {
            N.copy(a[i], 0, c, i * cols, cols);
        }

        return IntList.of(c);
    }

    /**
     * Flat op.
     *
     * @param <E> the element type
     * @param op the op
     * @throws E the e
     */
    @Override
    public <E extends Exception> void flatOp(Consumer<int[], E> op) throws E {
        f.flatOp(a, op);
    }

    /**
     * <pre>
     * <code>
     * IntMatrix a = IntMatrix.of({{1, 2, 3}, {4, 5, 6});
     * IntMatrix b = IntMatrix.of({{7, 8, 9}, {10, 11, 12});
     * 
     * IntMatrix c = a.vstack(b);
     * 
     * [[1, 2, 3],
     *  [4, 5, 6],
     *  [7, 8, 9],
     *  [10, 11, 12]]
     * 
     * </code>
     * </pre>
     *
     * @param b the b
     * @return the int matrix
     */
    public IntMatrix vstack(final IntMatrix b) {
        N.checkArgument(this.cols == b.cols, "The count of column in this matrix and the specified matrix are not equals");

        final int[][] c = new int[this.rows + b.rows][];
        int j = 0;

        for (int i = 0; i < rows; i++) {
            c[j++] = a[i].clone();
        }

        for (int i = 0; i < b.rows; i++) {
            c[j++] = b.a[i].clone();
        }

        return IntMatrix.of(c);
    }

    /**
     * <pre>
     * <code>
     * IntMatrix a = IntMatrix.of({{1, 2, 3}, {4, 5, 6});
     * IntMatrix b = IntMatrix.of({{7, 8, 9}, {10, 11, 12});
     * 
     * IntMatrix c = a.hstack(b);
     * 
     * [[1, 2, 3, 7, 8, 9],
     *  [4, 5, 6, 10, 11, 23]]
     * 
     * </code>
     * </pre>
     *
     * @param b the b
     * @return the int matrix
     */
    public IntMatrix hstack(final IntMatrix b) {
        N.checkArgument(this.rows == b.rows, "The count of row in this matrix and the specified matrix are not equals");

        final int[][] c = new int[rows][cols + b.cols];

        for (int i = 0; i < rows; i++) {
            N.copy(a[i], 0, c[i], 0, cols);
            N.copy(b.a[i], 0, c[i], cols, b.cols);
        }

        return IntMatrix.of(c);
    }

    /**
     * Adds the.
     *
     * @param b the b
     * @return the int matrix
     */
    public IntMatrix add(final IntMatrix b) {
        N.checkArgument(this.rows == b.rows && this.cols == b.cols, "The 'n' and length are not equal");

        final int[][] c = new int[rows][cols];

        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new IntConsumer() {
                    @Override
                    public void accept(final int i) {
                        for (int j = 0; j < cols; j++) {
                            c[i][j] = a[i][j] + b.a[i][j];
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new IntConsumer() {
                    @Override
                    public void accept(final int j) {
                        for (int i = 0; i < rows; i++) {
                            c[i][j] = a[i][j] + b.a[i][j];
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        c[i][j] = a[i][j] + b.a[i][j];
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        c[i][j] = a[i][j] + b.a[i][j];
                    }
                }
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Subtract.
     *
     * @param b the b
     * @return the int matrix
     */
    public IntMatrix subtract(final IntMatrix b) {
        N.checkArgument(this.rows == b.rows && this.cols == b.cols, "The 'n' and length are not equal");

        final int[][] c = new int[rows][cols];

        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new IntConsumer() {
                    @Override
                    public void accept(final int i) {
                        for (int j = 0; j < cols; j++) {
                            c[i][j] = a[i][j] - b.a[i][j];
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new IntConsumer() {
                    @Override
                    public void accept(final int j) {
                        for (int i = 0; i < rows; i++) {
                            c[i][j] = a[i][j] - b.a[i][j];
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        c[i][j] = a[i][j] - b.a[i][j];
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        c[i][j] = a[i][j] - b.a[i][j];
                    }
                }
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Multiply.
     *
     * @param b the b
     * @return the int matrix
     */
    public IntMatrix multiply(final IntMatrix b) {
        N.checkArgument(this.cols == b.rows, "Illegal matrix dimensions");

        final int[][] c = new int[rows][b.cols];
        final int[][] a2 = b.a;

        if (isParallelable(b.cols)) {
            if (N.min(rows, cols, b.cols) == rows) {
                if (N.min(cols, b.cols) == cols) {
                    IntStream.range(0, rows).parallel().forEach(new IntConsumer() {
                        @Override
                        public void accept(final int i) {
                            for (int k = 0; k < cols; k++) {
                                for (int j = 0; j < b.cols; j++) {
                                    c[i][j] += a[i][k] * a2[k][j];
                                }
                            }
                        }
                    });
                } else {
                    IntStream.range(0, rows).parallel().forEach(new IntConsumer() {
                        @Override
                        public void accept(final int i) {
                            for (int j = 0; j < b.cols; j++) {
                                for (int k = 0; k < cols; k++) {
                                    c[i][j] += a[i][k] * a2[k][j];
                                }
                            }
                        }
                    });
                }
            } else if (N.min(rows, cols, b.cols) == cols) {
                if (N.min(rows, b.cols) == rows) {
                    IntStream.range(0, cols).parallel().forEach(new IntConsumer() {
                        @Override
                        public void accept(final int k) {
                            for (int i = 0; i < rows; i++) {
                                for (int j = 0; j < b.cols; j++) {
                                    c[i][j] += a[i][k] * a2[k][j];
                                }
                            }
                        }
                    });
                } else {
                    IntStream.range(0, cols).parallel().forEach(new IntConsumer() {
                        @Override
                        public void accept(final int k) {
                            for (int j = 0; j < b.cols; j++) {
                                for (int i = 0; i < rows; i++) {
                                    c[i][j] += a[i][k] * a2[k][j];
                                }
                            }
                        }
                    });
                }
            } else {
                if (N.min(rows, cols) == rows) {
                    IntStream.range(0, b.cols).parallel().forEach(new IntConsumer() {
                        @Override
                        public void accept(final int j) {
                            for (int i = 0; i < rows; i++) {
                                for (int k = 0; k < cols; k++) {
                                    c[i][j] += a[i][k] * a2[k][j];
                                }
                            }
                        }
                    });
                } else {
                    IntStream.range(0, b.cols).parallel().forEach(new IntConsumer() {
                        @Override
                        public void accept(final int j) {
                            for (int k = 0; k < cols; k++) {
                                for (int i = 0; i < rows; i++) {
                                    c[i][j] += a[i][k] * a2[k][j];
                                }
                            }
                        }
                    });
                }
            }
        } else {
            if (N.min(rows, cols, b.cols) == rows) {
                if (N.min(cols, b.cols) == cols) {
                    for (int i = 0; i < rows; i++) {
                        for (int k = 0; k < cols; k++) {
                            for (int j = 0; j < b.cols; j++) {
                                c[i][j] += a[i][k] * a2[k][j];
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < rows; i++) {
                        for (int j = 0; j < b.cols; j++) {
                            for (int k = 0; k < cols; k++) {
                                c[i][j] += a[i][k] * a2[k][j];
                            }
                        }
                    }
                }
            } else if (N.min(rows, cols, b.cols) == cols) {
                if (N.min(rows, b.cols) == rows) {
                    for (int k = 0; k < cols; k++) {
                        for (int i = 0; i < rows; i++) {
                            for (int j = 0; j < b.cols; j++) {
                                c[i][j] += a[i][k] * a2[k][j];
                            }
                        }
                    }
                } else {
                    for (int k = 0; k < cols; k++) {
                        for (int j = 0; j < b.cols; j++) {
                            for (int i = 0; i < rows; i++) {
                                c[i][j] += a[i][k] * a2[k][j];
                            }
                        }
                    }
                }
            } else {
                if (N.min(rows, cols) == rows) {
                    for (int j = 0; j < b.cols; j++) {
                        for (int i = 0; i < rows; i++) {
                            for (int k = 0; k < cols; k++) {
                                c[i][j] += a[i][k] * a2[k][j];
                            }
                        }
                    }
                } else {
                    for (int j = 0; j < b.cols; j++) {
                        for (int k = 0; k < cols; k++) {
                            for (int i = 0; i < rows; i++) {
                                c[i][j] += a[i][k] * a2[k][j];
                            }
                        }
                    }
                }
            }
        }

        return new IntMatrix(c);
    }

    /**
     * Boxed.
     *
     * @return the matrix
     */
    public Matrix<Integer> boxed() {
        final Integer[][] c = new Integer[rows][cols];

        if (rows <= cols) {
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    c[i][j] = a[i][j];
                }
            }
        } else {
            for (int j = 0; j < cols; j++) {
                for (int i = 0; i < rows; i++) {
                    c[i][j] = a[i][j];
                }
            }
        }

        return new Matrix<>(c);
    }

    /**
     * To long matrix.
     *
     * @return the long matrix
     */
    public LongMatrix toLongMatrix() {
        return LongMatrix.from(a);
    }

    /**
     * To float matrix.
     *
     * @return the float matrix
     */
    public FloatMatrix toFloatMatrix() {
        return FloatMatrix.from(a);
    }

    /**
     * To double matrix.
     *
     * @return the double matrix
     */
    public DoubleMatrix toDoubleMatrix() {
        return DoubleMatrix.from(a);
    }

    /**
     * Zip with.
     *
     * @param <E> the element type
     * @param matrixB the matrix B
     * @param zipFunction the zip function
     * @return the int matrix
     * @throws E the e
     */
    public <E extends Exception> IntMatrix zipWith(final IntMatrix matrixB, final Try.IntBiFunction<Integer, E> zipFunction) throws E {
        N.checkArgument(isSameShape(matrixB), "Can't zip two matrices which have different shape.");

        final int[][] result = new int[rows][cols];
        final int[][] b = matrixB.a;

        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            result[i][j] = zipFunction.apply(a[i][j], b[i][j]);
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            result[i][j] = zipFunction.apply(a[i][j], b[i][j]);
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        result[i][j] = zipFunction.apply(a[i][j], b[i][j]);
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        result[i][j] = zipFunction.apply(a[i][j], b[i][j]);
                    }
                }
            }
        }

        return new IntMatrix(result);
    }

    /**
     * Zip with.
     *
     * @param <E> the element type
     * @param matrixB the matrix B
     * @param matrixC the matrix C
     * @param zipFunction the zip function
     * @return the int matrix
     * @throws E the e
     */
    public <E extends Exception> IntMatrix zipWith(final IntMatrix matrixB, final IntMatrix matrixC, final Try.IntTriFunction<Integer, E> zipFunction)
            throws E {
        N.checkArgument(isSameShape(matrixB) && isSameShape(matrixC), "Can't zip three matrices which have different shape.");

        final int[][] result = new int[rows][cols];
        final int[][] b = matrixB.a;
        final int[][] c = matrixC.a;

        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            result[i][j] = zipFunction.apply(a[i][j], b[i][j], c[i][j]);
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            result[i][j] = zipFunction.apply(a[i][j], b[i][j], c[i][j]);
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        result[i][j] = zipFunction.apply(a[i][j], b[i][j], c[i][j]);
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        result[i][j] = zipFunction.apply(a[i][j], b[i][j], c[i][j]);
                    }
                }
            }
        }

        return new IntMatrix(result);
    }

    /**
     * Stream LU 2 RD.
     *
     * @return a stream composed by elements on the diagonal line from left up to right down.
     */
    @Override
    public IntStream streamLU2RD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        if (isEmpty()) {
            return IntStream.empty();
        }

        return IntStream.of(new IntIteratorEx() {
            private final int toIndex = rows;
            private int cursor = 0;

            @Override
            public boolean hasNext() {
                return cursor < toIndex;
            }

            @Override
            public int nextInt() {
                if (cursor >= toIndex) {
                    throw new NoSuchElementException();
                }

                return a[cursor][cursor++];
            }

            @Override
            public void skip(long n) {
                N.checkArgNotNegative(n, "n");

                cursor = n < toIndex - cursor ? cursor + (int) n : toIndex;
            }

            @Override
            public long count() {
                return toIndex - cursor;
            }
        });
    }

    /**
     * Stream RU 2 LD.
     *
     * @return a stream composed by elements on the diagonal line from right up to left down.
     */
    @Override
    public IntStream streamRU2LD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        if (isEmpty()) {
            return IntStream.empty();
        }

        return IntStream.of(new IntIteratorEx() {
            private final int toIndex = rows;
            private int cursor = 0;

            @Override
            public boolean hasNext() {
                return cursor < toIndex;
            }

            @Override
            public int nextInt() {
                if (cursor >= toIndex) {
                    throw new NoSuchElementException();
                }

                return a[cursor][rows - ++cursor];
            }

            @Override
            public void skip(long n) {
                N.checkArgNotNegative(n, "n");

                cursor = n < toIndex - cursor ? cursor + (int) n : toIndex;
            }

            @Override
            public long count() {
                return toIndex - cursor;
            }
        });
    }

    /**
     * Stream H.
     *
     * @return a stream based on the order of row.
     */
    @Override
    public IntStream streamH() {
        return streamH(0, rows);
    }

    /**
     * Stream H.
     *
     * @param rowIndex the row index
     * @return the int stream
     */
    @Override
    public IntStream streamH(final int rowIndex) {
        return streamH(rowIndex, rowIndex + 1);
    }

    /**
     * Stream H.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @return a stream based on the order of row.
     */
    @Override
    public IntStream streamH(final int fromRowIndex, final int toRowIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);

        if (isEmpty()) {
            return IntStream.empty();
        }

        return IntStream.of(new IntIteratorEx() {
            private int i = fromRowIndex;
            private int j = 0;

            @Override
            public boolean hasNext() {
                return i < toRowIndex;
            }

            @Override
            public int nextInt() {
                if (i >= toRowIndex) {
                    throw new NoSuchElementException();
                }

                final int result = a[i][j++];

                if (j >= cols) {
                    i++;
                    j = 0;
                }

                return result;
            }

            @Override
            public void skip(long n) {
                N.checkArgNotNegative(n, "n");

                if (n >= (toRowIndex - i) * cols * 1L - j) {
                    i = toRowIndex;
                    j = 0;
                } else {
                    i += (n + j) / cols;
                    j += (n + j) % cols;
                }
            }

            @Override
            public long count() {
                return (toRowIndex - i) * cols * 1L - j;
            }

            @Override
            public int[] toArray() {
                final int len = (int) count();
                final int[] c = new int[len];

                for (int k = 0; k < len; k++) {
                    c[k] = a[i][j++];

                    if (j >= cols) {
                        i++;
                        j = 0;
                    }
                }

                return c;
            }
        });
    }

    /**
     * Stream V.
     *
     * @return a stream based on the order of column.
     */
    @Override
    @Beta
    public IntStream streamV() {
        return streamV(0, cols);
    }

    /**
     * Stream V.
     *
     * @param columnIndex the column index
     * @return the int stream
     */
    @Override
    public IntStream streamV(final int columnIndex) {
        return streamV(columnIndex, columnIndex + 1);
    }

    /**
     * Stream V.
     *
     * @param fromColumnIndex the from column index
     * @param toColumnIndex the to column index
     * @return a stream based on the order of column.
     */
    @Override
    @Beta
    public IntStream streamV(final int fromColumnIndex, final int toColumnIndex) {
        N.checkFromToIndex(fromColumnIndex, toColumnIndex, cols);

        if (isEmpty()) {
            return IntStream.empty();
        }

        return IntStream.of(new IntIteratorEx() {
            private int i = 0;
            private int j = fromColumnIndex;

            @Override
            public boolean hasNext() {
                return j < toColumnIndex;
            }

            @Override
            public int nextInt() {
                if (j >= toColumnIndex) {
                    throw new NoSuchElementException();
                }

                final int result = a[i++][j];

                if (i >= rows) {
                    i = 0;
                    j++;
                }

                return result;
            }

            @Override
            public void skip(long n) {
                N.checkArgNotNegative(n, "n");

                if (n >= (toColumnIndex - j) * IntMatrix.this.rows * 1L - i) {
                    i = 0;
                    j = toColumnIndex;
                } else {
                    i += (n + i) % IntMatrix.this.rows;
                    j += (n + i) / IntMatrix.this.rows;
                }
            }

            @Override
            public long count() {
                return (toColumnIndex - j) * rows - i;
            }

            @Override
            public int[] toArray() {
                final int len = (int) count();
                final int[] c = new int[len];

                for (int k = 0; k < len; k++) {
                    c[k] = a[i++][j];

                    if (i >= rows) {
                        i = 0;
                        j++;
                    }
                }

                return c;
            }
        });
    }

    /**
     * Stream R.
     *
     * @return a row stream based on the order of row.
     */
    @Override
    public Stream<IntStream> streamR() {
        return streamR(0, rows);
    }

    /**
     * Stream R.
     *
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @return a row stream based on the order of row.
     */
    @Override
    public Stream<IntStream> streamR(final int fromRowIndex, final int toRowIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);

        if (isEmpty()) {
            return Stream.empty();
        }

        return Stream.of(new ObjIteratorEx<IntStream>() {
            private final int toIndex = toRowIndex;
            private int cursor = fromRowIndex;

            @Override
            public boolean hasNext() {
                return cursor < toIndex;
            }

            @Override
            public IntStream next() {
                if (cursor >= toIndex) {
                    throw new NoSuchElementException();
                }

                return IntStream.of(a[cursor++]);
            }

            @Override
            public void skip(long n) {
                N.checkArgNotNegative(n, "n");

                cursor = n < toIndex - cursor ? cursor + (int) n : toIndex;
            }

            @Override
            public long count() {
                return toIndex - cursor;
            }
        });
    }

    /**
     * Stream C.
     *
     * @return a column stream based on the order of column.
     */
    @Override
    @Beta
    public Stream<IntStream> streamC() {
        return streamC(0, cols);
    }

    /**
     * Stream C.
     *
     * @param fromColumnIndex the from column index
     * @param toColumnIndex the to column index
     * @return a column stream based on the order of column.
     */
    @Override
    @Beta
    public Stream<IntStream> streamC(final int fromColumnIndex, final int toColumnIndex) {
        N.checkFromToIndex(fromColumnIndex, toColumnIndex, cols);

        if (isEmpty()) {
            return Stream.empty();
        }

        return Stream.of(new ObjIteratorEx<IntStream>() {
            private final int toIndex = toColumnIndex;
            private volatile int cursor = fromColumnIndex;

            @Override
            public boolean hasNext() {
                return cursor < toIndex;
            }

            @Override
            public IntStream next() {
                if (cursor >= toIndex) {
                    throw new NoSuchElementException();
                }

                return IntStream.of(new IntIteratorEx() {
                    private final int columnIndex = cursor++;
                    private final int toIndex2 = rows;
                    private int cursor2 = 0;

                    @Override
                    public boolean hasNext() {
                        return cursor2 < toIndex2;
                    }

                    @Override
                    public int nextInt() {
                        if (cursor2 >= toIndex2) {
                            throw new NoSuchElementException();
                        }

                        return a[cursor2++][columnIndex];
                    }

                    @Override
                    public void skip(long n) {
                        N.checkArgNotNegative(n, "n");

                        cursor2 = n < toIndex2 - cursor2 ? cursor2 + (int) n : toIndex2;
                    }

                    @Override
                    public long count() {
                        return toIndex2 - cursor2;
                    }
                });
            }

            @Override
            public void skip(long n) {
                N.checkArgNotNegative(n, "n");

                cursor = n < toIndex - cursor ? cursor + (int) n : toIndex;
            }

            @Override
            public long count() {
                return toIndex - cursor;
            }
        });
    }

    /**
     * Length.
     *
     * @param a the a
     * @return the int
     */
    @Override
    protected int length(int[] a) {
        return a == null ? 0 : a.length;
    }

    /**
     * For each.
     *
     * @param <E> the element type
     * @param action the action
     * @throws E the e
     */
    public <E extends Exception> void forEach(final Try.IntConsumer<E> action) throws E {
        forEach(0, rows, 0, cols, action);
    }

    /**
     * For each.
     *
     * @param <E> the element type
     * @param fromRowIndex the from row index
     * @param toRowIndex the to row index
     * @param fromColumnIndex the from column index
     * @param toColumnIndex the to column index
     * @param action the action
     * @throws E the e
     */
    public <E extends Exception> void forEach(final int fromRowIndex, final int toRowIndex, final int fromColumnIndex, final int toColumnIndex,
            final Try.IntConsumer<E> action) throws E {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);
        N.checkFromToIndex(fromColumnIndex, toColumnIndex, cols);

        for (int i = fromRowIndex; i < toRowIndex; i++) {
            for (int j = fromColumnIndex; j < toColumnIndex; j++) {
                action.accept(a[i][j]);
            }
        }
    }

    /**
     * Println.
     */
    @Override
    public void println() {
        f.println(a);
    }

    /**
     * Hash code.
     *
     * @return the int
     */
    @Override
    public int hashCode() {
        return N.deepHashCode(a);
    }

    /**
     * Equals.
     *
     * @param obj the obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof IntMatrix) {
            final IntMatrix another = (IntMatrix) obj;

            return N.deepEquals(this.a, another.a);
        }

        return false;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        return N.deepToString(a);
    }
}
