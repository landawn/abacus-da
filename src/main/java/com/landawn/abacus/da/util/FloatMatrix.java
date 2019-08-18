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
import com.landawn.abacus.util.FloatList;
import com.landawn.abacus.util.IntPair;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Try;
import com.landawn.abacus.util.Try.Consumer;
import com.landawn.abacus.util.u.OptionalFloat;
import com.landawn.abacus.util.function.IntConsumer;
import com.landawn.abacus.util.stream.FloatIteratorEx;
import com.landawn.abacus.util.stream.FloatStream;
import com.landawn.abacus.util.stream.IntStream;
import com.landawn.abacus.util.stream.ObjIteratorEx;
import com.landawn.abacus.util.stream.Stream;

// TODO: Auto-generated Javadoc
/**
 * The Class FloatMatrix.
 *
 * @author Haiyang Li
 * @since 0.8
 */
public final class FloatMatrix extends AbstractMatrix<float[], FloatList, FloatStream, Stream<FloatStream>, FloatMatrix> {

    /** The Constant EMPTY_FLOAT_MATRIX. */
    static final FloatMatrix EMPTY_FLOAT_MATRIX = new FloatMatrix(new float[0][0]);

    /**
     * Instantiates a new float matrix.
     *
     * @param a
     */
    public FloatMatrix(final float[][] a) {
        super(a == null ? new float[0][0] : a);
    }

    /**
     * Empty.
     *
     * @return
     */
    public static FloatMatrix empty() {
        return EMPTY_FLOAT_MATRIX;
    }

    /**
     * Of.
     *
     * @param a
     * @return
     */
    @SafeVarargs
    public static FloatMatrix of(final float[]... a) {
        return N.isNullOrEmpty(a) ? EMPTY_FLOAT_MATRIX : new FloatMatrix(a);
    }

    /**
     * From.
     *
     * @param a
     * @return
     */
    @SafeVarargs
    public static FloatMatrix from(final int[]... a) {
        if (N.isNullOrEmpty(a)) {
            return EMPTY_FLOAT_MATRIX;
        }

        final float[][] c = new float[a.length][a[0].length];

        for (int i = 0, len = a.length; i < len; i++) {
            for (int j = 0, col = a[0].length; j < col; j++) {
                c[i][j] = a[i][j];
            }
        }

        return new FloatMatrix(c);
    }

    /**
     * Random.
     *
     * @param len
     * @return
     */
    public static FloatMatrix random(final int len) {
        return new FloatMatrix(new float[][] { FloatList.random(len).array() });
    }

    /**
     * Repeat.
     *
     * @param val
     * @param len
     * @return
     */
    public static FloatMatrix repeat(final float val, final int len) {
        return new FloatMatrix(new float[][] { Array.repeat(val, len) });
    }

    /**
     * Diagonal LU 2 RD.
     *
     * @param leftUp2RighDownDiagonal
     * @return
     */
    public static FloatMatrix diagonalLU2RD(final float[] leftUp2RighDownDiagonal) {
        return diagonal(leftUp2RighDownDiagonal, null);
    }

    /**
     * Diagonal RU 2 LD.
     *
     * @param rightUp2LeftDownDiagonal
     * @return
     */
    public static FloatMatrix diagonalRU2LD(final float[] rightUp2LeftDownDiagonal) {
        return diagonal(null, rightUp2LeftDownDiagonal);
    }

    /**
     * Diagonal.
     *
     * @param leftUp2RighDownDiagonal
     * @param rightUp2LeftDownDiagonal
     * @return
     */
    public static FloatMatrix diagonal(final float[] leftUp2RighDownDiagonal, float[] rightUp2LeftDownDiagonal) {
        N.checkArgument(
                N.isNullOrEmpty(leftUp2RighDownDiagonal) || N.isNullOrEmpty(rightUp2LeftDownDiagonal)
                        || leftUp2RighDownDiagonal.length == rightUp2LeftDownDiagonal.length,
                "The length of 'leftUp2RighDownDiagonal' and 'rightUp2LeftDownDiagonal' must be same");

        if (N.isNullOrEmpty(leftUp2RighDownDiagonal)) {
            if (N.isNullOrEmpty(rightUp2LeftDownDiagonal)) {
                return empty();
            } else {
                final int len = rightUp2LeftDownDiagonal.length;
                final float[][] c = new float[len][len];

                for (int i = 0, j = len - 1; i < len; i++, j--) {
                    c[i][j] = rightUp2LeftDownDiagonal[i];
                }

                return new FloatMatrix(c);
            }
        } else {
            final int len = leftUp2RighDownDiagonal.length;
            final float[][] c = new float[len][len];

            for (int i = 0; i < len; i++) {
                c[i][i] = leftUp2RighDownDiagonal[i];
            }

            if (N.notNullOrEmpty(rightUp2LeftDownDiagonal)) {
                for (int i = 0, j = len - 1; i < len; i++, j--) {
                    c[i][j] = rightUp2LeftDownDiagonal[i];
                }
            }

            return new FloatMatrix(c);
        }
    }

    /**
     * Gets the.
     *
     * @param i
     * @param j
     * @return
     */
    public float get(final int i, final int j) {
        return a[i][j];
    }

    /**
     * Gets the.
     *
     * @param point
     * @return
     */
    public float get(final IntPair point) {
        return a[point._1][point._2];
    }

    /**
     * Sets the.
     *
     * @param i
     * @param j
     * @param val
     */
    public void set(final int i, final int j, final float val) {
        a[i][j] = val;
    }

    /**
     * Sets the.
     *
     * @param point
     * @param val
     */
    public void set(final IntPair point, final float val) {
        a[point._1][point._2] = val;
    }

    /**
     * Up of.
     *
     * @param i
     * @param j
     * @return
     */
    public OptionalFloat upOf(final int i, final int j) {
        return i == 0 ? OptionalFloat.empty() : OptionalFloat.of(a[i - 1][j]);
    }

    /**
     * Down of.
     *
     * @param i
     * @param j
     * @return
     */
    public OptionalFloat downOf(final int i, final int j) {
        return i == rows - 1 ? OptionalFloat.empty() : OptionalFloat.of(a[i + 1][j]);
    }

    /**
     * Left of.
     *
     * @param i
     * @param j
     * @return
     */
    public OptionalFloat leftOf(final int i, final int j) {
        return j == 0 ? OptionalFloat.empty() : OptionalFloat.of(a[i][j - 1]);
    }

    /**
     * Right of.
     *
     * @param i
     * @param j
     * @return
     */
    public OptionalFloat rightOf(final int i, final int j) {
        return j == cols - 1 ? OptionalFloat.empty() : OptionalFloat.of(a[i][j + 1]);
    }

    /**
     * Returns the four adjacencies with order: up, right, down, left. <code>null</code> is set if the adjacency doesn't exist.
     *
     * @param i
     * @param j
     * @return
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
     * @param i
     * @param j
     * @return
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
     * @param rowIndex
     * @return
     */
    public float[] row(final int rowIndex) {
        N.checkArgument(rowIndex >= 0 && rowIndex < rows, "Invalid row Index: %s", rowIndex);

        return a[rowIndex];
    }

    /**
     * Column.
     *
     * @param columnIndex
     * @return
     */
    public float[] column(final int columnIndex) {
        N.checkArgument(columnIndex >= 0 && columnIndex < cols, "Invalid column Index: %s", columnIndex);

        final float[] c = new float[rows];

        for (int i = 0; i < rows; i++) {
            c[i] = a[i][columnIndex];
        }

        return c;
    }

    /**
     * Sets the row.
     *
     * @param rowIndex
     * @param row
     */
    public void setRow(int rowIndex, float[] row) {
        N.checkArgument(row.length == cols, "The size of the specified row doesn't match the length of column");

        N.copy(row, 0, a[rowIndex], 0, cols);
    }

    /**
     * Sets the column.
     *
     * @param columnIndex
     * @param column
     */
    public void setColumn(int columnIndex, float[] column) {
        N.checkArgument(column.length == rows, "The size of the specified column doesn't match the length of row");

        for (int i = 0; i < rows; i++) {
            a[i][columnIndex] = column[i];
        }
    }

    /**
     * Update row.
     *
     * @param <E> the element type
     * @param rowIndex
     * @param func
     * @throws E the e
     */
    public <E extends Exception> void updateRow(int rowIndex, Try.FloatUnaryOperator<E> func) throws E {
        for (int i = 0; i < cols; i++) {
            a[rowIndex][i] = func.applyAsFloat(a[rowIndex][i]);
        }
    }

    /**
     * Update column.
     *
     * @param <E> the element type
     * @param columnIndex
     * @param func
     * @throws E the e
     */
    public <E extends Exception> void updateColumn(int columnIndex, Try.FloatUnaryOperator<E> func) throws E {
        for (int i = 0; i < rows; i++) {
            a[i][columnIndex] = func.applyAsFloat(a[i][columnIndex]);
        }
    }

    /**
     * Gets the lu2rd.
     *
     * @return
     */
    public float[] getLU2RD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        final float[] res = new float[rows];

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
    public void setLU2RD(final float[] diagonal) {
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
     * @param func
     * @throws E the e
     */
    public <E extends Exception> void updateLU2RD(final Try.FloatUnaryOperator<E> func) throws E {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        for (int i = 0; i < rows; i++) {
            a[i][i] = func.applyAsFloat(a[i][i]);
        }
    }

    /**
     * Gets the ru2ld.
     *
     * @return
     */
    public float[] getRU2LD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        final float[] res = new float[rows];

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
    public void setRU2LD(final float[] diagonal) {
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
     * @param func
     * @throws E the e
     */
    public <E extends Exception> void updateRU2LD(final Try.FloatUnaryOperator<E> func) throws E {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        for (int i = 0; i < rows; i++) {
            a[i][cols - i - 1] = func.applyAsFloat(a[i][cols - i - 1]);
        }
    }

    /**
     * Update all.
     *
     * @param <E> the element type
     * @param func
     * @throws E the e
     */
    public <E extends Exception> void updateAll(final Try.FloatUnaryOperator<E> func) throws E {
        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            a[i][j] = func.applyAsFloat(a[i][j]);
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            a[i][j] = func.applyAsFloat(a[i][j]);
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        a[i][j] = func.applyAsFloat(a[i][j]);
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        a[i][j] = func.applyAsFloat(a[i][j]);
                    }
                }
            }
        }
    }

    /**
     * Update all elements based on points.
     *
     * @param <E> the element type
     * @param func
     * @throws E the e
     */
    public <E extends Exception> void updateAll(final Try.IntBiFunction<Float, E> func) throws E {
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
     * @param predicate
     * @param newValue
     * @throws E the e
     */
    public <E extends Exception> void replaceIf(final Try.FloatPredicate<E> predicate, final float newValue) throws E {
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
     * @param predicate
     * @param newValue
     * @throws E the e
     */
    public <E extends Exception> void replaceIf(final Try.IntBiPredicate<E> predicate, final float newValue) throws E {
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
     * @param func
     * @return
     * @throws E the e
     */
    public <E extends Exception> FloatMatrix map(final Try.FloatUnaryOperator<E> func) throws E {
        final float[][] c = new float[rows][cols];

        if (isParallelable()) {
            if (rows <= cols) {
                IntStream.range(0, rows).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int i) throws E {
                        for (int j = 0; j < cols; j++) {
                            c[i][j] = func.applyAsFloat(a[i][j]);
                        }
                    }
                });
            } else {
                IntStream.range(0, cols).parallel().forEach(new Try.IntConsumer<E>() {
                    @Override
                    public void accept(final int j) throws E {
                        for (int i = 0; i < rows; i++) {
                            c[i][j] = func.applyAsFloat(a[i][j]);
                        }
                    }
                });
            }
        } else {
            if (rows <= cols) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        c[i][j] = func.applyAsFloat(a[i][j]);
                    }
                }
            } else {
                for (int j = 0; j < cols; j++) {
                    for (int i = 0; i < rows; i++) {
                        c[i][j] = func.applyAsFloat(a[i][j]);
                    }
                }
            }
        }

        return FloatMatrix.of(c);
    }

    /**
     * Map to obj.
     *
     * @param <T>
     * @param <E> the element type
     * @param cls
     * @param func
     * @return
     * @throws E the e
     */
    public <T, E extends Exception> Matrix<T> mapToObj(final Class<T> cls, final Try.FloatFunction<? extends T, E> func) throws E {
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
     * @param val
     */
    public void fill(final float val) {
        for (int i = 0; i < rows; i++) {
            N.fill(a[i], val);
        }
    }

    /**
     * Fill.
     *
     * @param b
     */
    public void fill(final float[][] b) {
        fill(0, 0, b);
    }

    /**
     * Fill.
     *
     * @param fromRowIndex
     * @param fromColumnIndex
     * @param b
     */
    public void fill(final int fromRowIndex, final int fromColumnIndex, final float[][] b) {
        N.checkFromToIndex(fromRowIndex, rows, rows);
        N.checkFromToIndex(fromColumnIndex, cols, cols);

        for (int i = 0, minLen = N.min(rows - fromRowIndex, b.length); i < minLen; i++) {
            N.copy(b[i], 0, a[i + fromRowIndex], fromColumnIndex, N.min(b[i].length, cols - fromColumnIndex));
        }
    }

    /**
     * Copy.
     *
     * @return
     */
    @Override
    public FloatMatrix copy() {
        final float[][] c = new float[rows][];

        for (int i = 0; i < rows; i++) {
            c[i] = a[i].clone();
        }

        return new FloatMatrix(c);
    }

    /**
     * Copy.
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @return
     */
    @Override
    public FloatMatrix copy(final int fromRowIndex, final int toRowIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);

        final float[][] c = new float[toRowIndex - fromRowIndex][];

        for (int i = fromRowIndex; i < toRowIndex; i++) {
            c[i - fromRowIndex] = a[i].clone();
        }

        return new FloatMatrix(c);
    }

    /**
     * Copy.
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @param fromColumnIndex
     * @param toColumnIndex
     * @return
     */
    @Override
    public FloatMatrix copy(final int fromRowIndex, final int toRowIndex, final int fromColumnIndex, final int toColumnIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);
        N.checkFromToIndex(fromColumnIndex, toColumnIndex, cols);

        final float[][] c = new float[toRowIndex - fromRowIndex][];

        for (int i = fromRowIndex; i < toRowIndex; i++) {
            c[i - fromRowIndex] = N.copyOfRange(a[i], fromColumnIndex, toColumnIndex);
        }

        return new FloatMatrix(c);
    }

    /**
     * Extend.
     *
     * @param newRows
     * @param newCols
     * @return
     */
    public FloatMatrix extend(final int newRows, final int newCols) {
        return extend(newRows, newCols, 0);
    }

    /**
     * Extend.
     *
     * @param newRows
     * @param newCols
     * @param defaultValueForNewCell
     * @return
     */
    public FloatMatrix extend(final int newRows, final int newCols, final float defaultValueForNewCell) {
        N.checkArgument(newRows >= 0, "The 'newRows' can't be negative %s", newRows);
        N.checkArgument(newCols >= 0, "The 'newCols' can't be negative %s", newCols);

        if (newRows <= rows && newCols <= cols) {
            return copy(0, newRows, 0, newCols);
        } else {
            final boolean fillDefaultValue = defaultValueForNewCell != 0;
            final float[][] b = new float[newRows][];

            for (int i = 0; i < newRows; i++) {
                b[i] = i < rows ? N.copyOf(a[i], newCols) : new float[newCols];

                if (fillDefaultValue) {
                    if (i >= rows) {
                        N.fill(b[i], defaultValueForNewCell);
                    } else if (cols < newCols) {
                        N.fill(b[i], cols, newCols, defaultValueForNewCell);
                    }
                }
            }

            return new FloatMatrix(b);
        }
    }

    /**
     * Extend.
     *
     * @param toUp
     * @param toDown
     * @param toLeft
     * @param toRight
     * @return
     */
    public FloatMatrix extend(final int toUp, final int toDown, final int toLeft, final int toRight) {
        return extend(toUp, toDown, toLeft, toRight, 0);
    }

    /**
     * Extend.
     *
     * @param toUp
     * @param toDown
     * @param toLeft
     * @param toRight
     * @param defaultValueForNewCell
     * @return
     */
    public FloatMatrix extend(final int toUp, final int toDown, final int toLeft, final int toRight, final float defaultValueForNewCell) {
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
            final float[][] b = new float[newRows][newCols];

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

            return new FloatMatrix(b);
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
            float tmp = 0;
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
     * @return
     * @see IntMatrix#flipH()
     */
    public FloatMatrix flipH() {
        final FloatMatrix res = this.copy();
        res.reverseH();
        return res;
    }

    /**
     * Flip V.
     *
     * @return
     * @see IntMatrix#flipV()
     */
    public FloatMatrix flipV() {
        final FloatMatrix res = this.copy();
        res.reverseV();
        return res;
    }

    /**
     * Rotate 90.
     *
     * @return
     */
    @Override
    public FloatMatrix rotate90() {
        final float[][] c = new float[cols][rows];

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

        return new FloatMatrix(c);
    }

    /**
     * Rotate 180.
     *
     * @return
     */
    @Override
    public FloatMatrix rotate180() {
        final float[][] c = new float[rows][];

        for (int i = 0; i < rows; i++) {
            c[i] = a[rows - i - 1].clone();
            N.reverse(c[i]);
        }

        return new FloatMatrix(c);
    }

    /**
     * Rotate 270.
     *
     * @return
     */
    @Override
    public FloatMatrix rotate270() {
        final float[][] c = new float[cols][rows];

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

        return new FloatMatrix(c);
    }

    /**
     * Transpose.
     *
     * @return
     */
    @Override
    public FloatMatrix transpose() {
        final float[][] c = new float[cols][rows];

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

        return new FloatMatrix(c);
    }

    /**
     * Reshape.
     *
     * @param newRows
     * @param newCols
     * @return
     */
    @Override
    public FloatMatrix reshape(final int newRows, final int newCols) {
        final float[][] c = new float[newRows][newCols];

        if (newRows == 0 || newCols == 0 || N.isNullOrEmpty(a)) {
            return new FloatMatrix(c);
        }

        if (a.length == 1) {
            final float[] a0 = a[0];

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

        return new FloatMatrix(c);
    }

    /**
     * Repeat elements <code>rowRepeats</code> times in row direction and <code>colRepeats</code> times in column direction.
     *
     * @param rowRepeats
     * @param colRepeats
     * @return a new matrix
     * @see IntMatrix#repelem(int, int)
     */
    @Override
    public FloatMatrix repelem(final int rowRepeats, final int colRepeats) {
        N.checkArgument(rowRepeats > 0 && colRepeats > 0, "rowRepeats=%s and colRepeats=%s must be bigger than 0", rowRepeats, colRepeats);

        final float[][] c = new float[rows * rowRepeats][cols * colRepeats];

        for (int i = 0; i < rows; i++) {
            final float[] fr = c[i * rowRepeats];

            for (int j = 0; j < cols; j++) {
                N.copy(Array.repeat(a[i][j], colRepeats), 0, fr, j * colRepeats, colRepeats);
            }

            for (int k = 1; k < rowRepeats; k++) {
                N.copy(fr, 0, c[i * rowRepeats + k], 0, fr.length);
            }
        }

        return new FloatMatrix(c);
    }

    /**
     * Repeat this matrix <code>rowRepeats</code> times in row direction and <code>colRepeats</code> times in column direction.
     *
     * @param rowRepeats
     * @param colRepeats
     * @return a new matrix
     * @see IntMatrix#repmat(int, int)
     */
    @Override
    public FloatMatrix repmat(final int rowRepeats, final int colRepeats) {
        N.checkArgument(rowRepeats > 0 && colRepeats > 0, "rowRepeats=%s and colRepeats=%s must be bigger than 0", rowRepeats, colRepeats);

        final float[][] c = new float[rows * rowRepeats][cols * colRepeats];

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

        return new FloatMatrix(c);
    }

    /**
     * Flatten.
     *
     * @return
     */
    @Override
    public FloatList flatten() {
        final float[] c = new float[rows * cols];

        for (int i = 0; i < rows; i++) {
            N.copy(a[i], 0, c, i * cols, cols);
        }

        return FloatList.of(c);
    }

    /**
     * Flat op.
     *
     * @param <E> the element type
     * @param op
     * @throws E the e
     */
    @Override
    public <E extends Exception> void flatOp(Consumer<float[], E> op) throws E {
        f.flatOp(a, op);
    }

    /**
     * Vstack.
     *
     * @param b
     * @return
     * @see IntMatrix#vstack(IntMatrix)
     */
    public FloatMatrix vstack(final FloatMatrix b) {
        N.checkArgument(this.cols == b.cols, "The count of column in this matrix and the specified matrix are not equals");

        final float[][] c = new float[this.rows + b.rows][];
        int j = 0;

        for (int i = 0; i < rows; i++) {
            c[j++] = a[i].clone();
        }

        for (int i = 0; i < b.rows; i++) {
            c[j++] = b.a[i].clone();
        }

        return FloatMatrix.of(c);
    }

    /**
     * Hstack.
     *
     * @param b
     * @return
     * @see IntMatrix#hstack(IntMatrix)
     */
    public FloatMatrix hstack(final FloatMatrix b) {
        N.checkArgument(this.rows == b.rows, "The count of row in this matrix and the specified matrix are not equals");

        final float[][] c = new float[rows][cols + b.cols];

        for (int i = 0; i < rows; i++) {
            N.copy(a[i], 0, c[i], 0, cols);
            N.copy(b.a[i], 0, c[i], cols, b.cols);
        }

        return FloatMatrix.of(c);
    }

    /**
     * Adds the.
     *
     * @param b
     * @return
     */
    public FloatMatrix add(final FloatMatrix b) {
        N.checkArgument(this.rows == b.rows && this.cols == b.cols, "The 'n' and length are not equal");

        final float[][] c = new float[rows][cols];

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

        return new FloatMatrix(c);
    }

    /**
     * Subtract.
     *
     * @param b
     * @return
     */
    public FloatMatrix subtract(final FloatMatrix b) {
        N.checkArgument(this.rows == b.rows && this.cols == b.cols, "The 'n' and length are not equal");

        final float[][] c = new float[rows][cols];

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

        return new FloatMatrix(c);
    }

    /**
     * Multiply.
     *
     * @param b
     * @return
     */
    public FloatMatrix multiply(final FloatMatrix b) {
        N.checkArgument(this.cols == b.rows, "Illegal matrix dimensions");

        final float[][] c = new float[rows][b.cols];
        final float[][] a2 = b.a;

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

        return new FloatMatrix(c);
    }

    /**
     * Boxed.
     *
     * @return
     */
    public Matrix<Float> boxed() {
        final Float[][] c = new Float[rows][cols];

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
     * To double matrix.
     *
     * @return
     */
    public DoubleMatrix toDoubleMatrix() {
        return DoubleMatrix.from(a);
    }

    /**
     * Zip with.
     *
     * @param <E> the element type
     * @param matrixB
     * @param zipFunction
     * @return
     * @throws E the e
     */
    public <E extends Exception> FloatMatrix zipWith(final FloatMatrix matrixB, final Try.FloatBiFunction<Float, E> zipFunction) throws E {
        N.checkArgument(isSameShape(matrixB), "Can't zip two matrices which have different shape.");

        final float[][] result = new float[rows][cols];
        final float[][] b = matrixB.a;

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

        return new FloatMatrix(result);
    }

    /**
     * Zip with.
     *
     * @param <E> the element type
     * @param matrixB
     * @param matrixC
     * @param zipFunction
     * @return
     * @throws E the e
     */
    public <E extends Exception> FloatMatrix zipWith(final FloatMatrix matrixB, final FloatMatrix matrixC, final Try.FloatTriFunction<Float, E> zipFunction)
            throws E {
        N.checkArgument(isSameShape(matrixB) && isSameShape(matrixC), "Can't zip three matrices which have different shape.");

        final float[][] result = new float[rows][cols];
        final float[][] b = matrixB.a;
        final float[][] c = matrixC.a;

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

        return new FloatMatrix(result);
    }

    /**
     * Stream LU 2 RD.
     *
     * @return a stream composed by elements on the diagonal line from left up to right down.
     */
    @Override
    public FloatStream streamLU2RD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        if (isEmpty()) {
            return FloatStream.empty();
        }

        return FloatStream.of(new FloatIteratorEx() {
            private final int toIndex = rows;
            private int cursor = 0;

            @Override
            public boolean hasNext() {
                return cursor < toIndex;
            }

            @Override
            public float nextFloat() {
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
    public FloatStream streamRU2LD() {
        N.checkState(rows == cols, "'rows' and 'cols' must be same to get diagonals: rows=%s, cols=%s", rows, cols);

        if (isEmpty()) {
            return FloatStream.empty();
        }

        return FloatStream.of(new FloatIteratorEx() {
            private final int toIndex = rows;
            private int cursor = 0;

            @Override
            public boolean hasNext() {
                return cursor < toIndex;
            }

            @Override
            public float nextFloat() {
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
    public FloatStream streamH() {
        return streamH(0, rows);
    }

    /**
     * Stream H.
     *
     * @param rowIndex
     * @return
     */
    @Override
    public FloatStream streamH(final int rowIndex) {
        return streamH(rowIndex, rowIndex + 1);
    }

    /**
     * Stream H.
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @return a stream based on the order of row.
     */
    @Override
    public FloatStream streamH(final int fromRowIndex, final int toRowIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);

        if (isEmpty()) {
            return FloatStream.empty();
        }

        return FloatStream.of(new FloatIteratorEx() {
            private int i = fromRowIndex;
            private int j = 0;

            @Override
            public boolean hasNext() {
                return i < toRowIndex;
            }

            @Override
            public float nextFloat() {
                if (i >= toRowIndex) {
                    throw new NoSuchElementException();
                }

                final float result = a[i][j++];

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
            public float[] toArray() {
                final int len = (int) count();
                final float[] c = new float[len];

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
    public FloatStream streamV() {
        return streamV(0, cols);
    }

    /**
     * Stream V.
     *
     * @param columnIndex
     * @return
     */
    @Override
    public FloatStream streamV(final int columnIndex) {
        return streamV(columnIndex, columnIndex + 1);
    }

    /**
     * Stream V.
     *
     * @param fromColumnIndex
     * @param toColumnIndex
     * @return a stream based on the order of column.
     */
    @Override
    @Beta
    public FloatStream streamV(final int fromColumnIndex, final int toColumnIndex) {
        N.checkFromToIndex(fromColumnIndex, toColumnIndex, cols);

        if (isEmpty()) {
            return FloatStream.empty();
        }

        return FloatStream.of(new FloatIteratorEx() {
            private int i = 0;
            private int j = fromColumnIndex;

            @Override
            public boolean hasNext() {
                return j < toColumnIndex;
            }

            @Override
            public float nextFloat() {
                if (j >= toColumnIndex) {
                    throw new NoSuchElementException();
                }

                final float result = a[i++][j];

                if (i >= rows) {
                    i = 0;
                    j++;
                }

                return result;
            }

            @Override
            public void skip(long n) {
                N.checkArgNotNegative(n, "n");

                if (n >= (toColumnIndex - j) * FloatMatrix.this.rows * 1L - i) {
                    i = 0;
                    j = toColumnIndex;
                } else {
                    i += (n + i) % FloatMatrix.this.rows;
                    j += (n + i) / FloatMatrix.this.rows;
                }
            }

            @Override
            public long count() {
                return (toColumnIndex - j) * rows - i;
            }

            @Override
            public float[] toArray() {
                final int len = (int) count();
                final float[] c = new float[len];

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
    public Stream<FloatStream> streamR() {
        return streamR(0, rows);
    }

    /**
     * Stream R.
     *
     * @param fromRowIndex
     * @param toRowIndex
     * @return a row stream based on the order of row.
     */
    @Override
    public Stream<FloatStream> streamR(final int fromRowIndex, final int toRowIndex) {
        N.checkFromToIndex(fromRowIndex, toRowIndex, rows);

        if (isEmpty()) {
            return Stream.empty();
        }

        return Stream.of(new ObjIteratorEx<FloatStream>() {
            private final int toIndex = toRowIndex;
            private int cursor = fromRowIndex;

            @Override
            public boolean hasNext() {
                return cursor < toIndex;
            }

            @Override
            public FloatStream next() {
                if (cursor >= toIndex) {
                    throw new NoSuchElementException();
                }

                return FloatStream.of(a[cursor++]);
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
    public Stream<FloatStream> streamC() {
        return streamC(0, cols);
    }

    /**
     * Stream C.
     *
     * @param fromColumnIndex
     * @param toColumnIndex
     * @return a column stream based on the order of column.
     */
    @Override
    @Beta
    public Stream<FloatStream> streamC(final int fromColumnIndex, final int toColumnIndex) {
        N.checkFromToIndex(fromColumnIndex, toColumnIndex, cols);

        if (isEmpty()) {
            return Stream.empty();
        }

        return Stream.of(new ObjIteratorEx<FloatStream>() {
            private final int toIndex = toColumnIndex;
            private volatile int cursor = fromColumnIndex;

            @Override
            public boolean hasNext() {
                return cursor < toIndex;
            }

            @Override
            public FloatStream next() {
                if (cursor >= toIndex) {
                    throw new NoSuchElementException();
                }

                return FloatStream.of(new FloatIteratorEx() {
                    private final int columnIndex = cursor++;
                    private final int toIndex2 = rows;
                    private int cursor2 = 0;

                    @Override
                    public boolean hasNext() {
                        return cursor2 < toIndex2;
                    }

                    @Override
                    public float nextFloat() {
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
     * @param a
     * @return
     */
    @Override
    protected int length(float[] a) {
        return a == null ? 0 : a.length;
    }

    /**
     * For each.
     *
     * @param <E> the element type
     * @param action
     * @throws E the e
     */
    public <E extends Exception> void forEach(final Try.FloatConsumer<E> action) throws E {
        forEach(0, rows, 0, cols, action);
    }

    /**
     * For each.
     *
     * @param <E> the element type
     * @param fromRowIndex
     * @param toRowIndex
     * @param fromColumnIndex
     * @param toColumnIndex
     * @param action
     * @throws E the e
     */
    public <E extends Exception> void forEach(final int fromRowIndex, final int toRowIndex, final int fromColumnIndex, final int toColumnIndex,
            final Try.FloatConsumer<E> action) throws E {
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
     * @return
     */
    @Override
    public int hashCode() {
        return N.deepHashCode(a);
    }

    /**
     * Equals.
     *
     * @param obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof FloatMatrix) {
            final FloatMatrix another = (FloatMatrix) obj;

            return N.deepEquals(this.a, another.a);
        }

        return false;
    }

    /**
     * To string.
     *
     * @return
     */
    @Override
    public String toString() {
        return N.deepToString(a);
    }
}
