//
//Copyright (C) 2014-2017 Łukasz Bownik
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
//associated documentation files (the "Software"), to deal in the Software without restriction, including 
//without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
//copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the 
//following conditions:
//
//The above copyright notice and this permission notice shall be included in all copies or substantial 
//portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT 
//LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
//IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
//HETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
//SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package fluentJDBC;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import static java.sql.Statement.*;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;

/*******************************************************************************
 *
 * @author lukasz.bownik@gmail.com
 ******************************************************************************/
public final class FluentJDBC {
   /***************************************************************************
    * 
    **************************************************************************/
   public static FluentJDBC using(final Connection c) {

      if (c == null) {
         throw new NullPointerException("Null connection.");
      }
      return new FluentJDBC(c);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   public Preparator prepare(final String sql) throws SQLException {

      return new Preparator(this.c.prepareStatement(sql));
   }
   /***************************************************************************
    * 
    **************************************************************************/
   public Preparator prepare(final String sql, final boolean returnGeneratedKeys)
           throws SQLException {

      return new Preparator(this.c.prepareStatement(sql,
              returnGeneratedKeys ? RETURN_GENERATED_KEYS : NO_GENERATED_KEYS));
   }
   /***************************************************************************
    * 
    **************************************************************************/
   public Connection unwrap() {
      
      return this.c;
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private FluentJDBC(final Connection c) {

      this.c = c;
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private final Connection c;

   /***************************************************************************
    * 
    **************************************************************************/
   public final static class Preparator implements AutoCloseable {

      /***********************************************************************
       * 
       **********************************************************************/
      private Preparator(final PreparedStatement s) {

         this.s = s;
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final byte v) throws SQLException {

         try {
            this.s.setByte(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final short v) throws SQLException {

         try {
            this.s.setShort(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final int v) throws SQLException {

         try {
            this.s.setInt(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final long v) throws SQLException {

         try {
            this.s.setLong(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final float v) throws SQLException {

         try {
            this.s.setFloat(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final double v) throws SQLException {

         try {
            this.s.setDouble(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final String v) throws SQLException {

         try {
            this.s.setString(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Date v) throws SQLException {

         try {
            this.s.setDate(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final boolean v) throws SQLException {

         try {
            this.s.setBoolean(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final BigDecimal v) throws SQLException {

         try {
            this.s.setBigDecimal(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final byte[] v) throws SQLException {

         try {
            this.s.setBytes(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Time v) throws SQLException {

         try {
            this.s.setTime(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Time v, final Calendar c)
              throws SQLException {

         try {
            this.s.setTime(++this.paramIndex, v, c);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Timestamp v) throws SQLException {

         try {
            this.s.setTimestamp(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Object v) throws SQLException {

         try {
            this.s.setObject(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Blob v) throws SQLException {

         try {
            this.s.setBlob(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Clob v) throws SQLException {

         try {
            this.s.setClob(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final NClob v) throws SQLException {

         try {
            this.s.setNClob(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Array v) throws SQLException {

         try {
            this.s.setArray(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Reader v) throws SQLException {

         try {
            this.s.setCharacterStream(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final InputStream v) throws SQLException {

         try {
            this.s.setBinaryStream(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final URL v) throws SQLException {

         try {
            this.s.setURL(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final RowId v) throws SQLException {

         try {
            this.s.setRowId(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final SQLXML v) throws SQLException {

         try {
            this.s.setSQLXML(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Ref v) throws SQLException {

         try {
            this.s.setRef(++this.paramIndex, v);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Setter setter) throws Exception {

         try {
            setter.set(this.s, ++this.paramIndex);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator apply(final Consumer<PreparedStatement, SQLException> consumer)
              throws SQLException {

         try {
            consumer.consume(this.s);
            return this;
         } catch (final SQLException e) {
            this.s.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public int andUpdate() throws SQLException {

         try {
            return this.s.executeUpdate();
         } finally {
            this.s.close();
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Object andUpdateReturningKey() throws SQLException {

         try {
            this.s.executeUpdate();
            try (final ResultSet rs = this.s.getGeneratedKeys()) {
               rs.next();
               return rs.getObject(1);
            }
         } finally {
            this.s.close();
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public <T> List<T> andMap(final Mapper<T> mapper)
              throws Exception {

         return andMap(10, mapper);
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public <T> List<T> andMap(final int size, final Mapper<T> mapper)
              throws Exception {

         final ArrayList<T> result = new ArrayList<>(size);
         try (final ResultSet rs = this.s.executeQuery()) {
            while (rs.next()) {
               result.add(mapper.map(rs));
            }
         } finally {
            this.s.close();
         }
         return result;
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public <T> Optional<T> andMapOne(final Mapper<T> mapper)
              throws Exception {

         try (final ResultSet rs = this.s.executeQuery()) {
            if (rs.next()) {
               return Optional.<T>of(mapper.map(rs));
            } else {
               return Optional.<T>empty();
            }
         } finally {
            this.s.close();
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public void andForEach(final Consumer<ResultSet, Exception> consumer)
              throws Exception {

         try (final ResultSet rs = this.s.executeQuery()) {
            while (rs.next()) {
               consumer.consume(rs);
            }
         } finally {
            this.s.close();
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public <T> T andReduce(T accumulator, final Reducer<T> reducer)
              throws Exception {

         try (final ResultSet rs = this.s.executeQuery()) {
            while (rs.next()) {
               accumulator = reducer.reduce(accumulator, rs);
            }
         } finally {
            this.s.close();
         }
         return accumulator;
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public void close() throws Exception {

         this.s.close();
      }
      /***********************************************************************
       * 
       **********************************************************************/
      private final PreparedStatement s;
      private int paramIndex = 0;
   }
   /***************************************************************************
    * 
    **************************************************************************/
   public interface Mapper<T> {

      /***********************************************************************
       * 
       **********************************************************************/
      T map(ResultSet rs) throws Exception;
   }

   /***************************************************************************
    * 
    **************************************************************************/
   public interface Consumer<T, E extends Throwable> {

      /***********************************************************************
       * 
       **********************************************************************/
      void consume(T o) throws E;
   }

   /***************************************************************************
    * 
    **************************************************************************/
   public interface Reducer<T> {

      /***********************************************************************
       * 
       **********************************************************************/
      T reduce(T seed, ResultSet rs) throws Exception;
   }
   /***************************************************************************
    * 
    **************************************************************************/
   public interface Setter {

      /***********************************************************************
       * 
       **********************************************************************/
      void set(PreparedStatement s, int index) throws Exception;
   }
}
