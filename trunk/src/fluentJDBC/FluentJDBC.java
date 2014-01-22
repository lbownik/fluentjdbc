//
//Copyright (C) 2014 Łukasz Bownik
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
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

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
   public final static class Preparator {

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
      public void update() throws SQLException {

         try {
            this.s.executeUpdate();
         } finally {
            this.s.close();
         }
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public <T> List<T> mapQuery(final RSetFunction<T> mapper)
              throws SQLException {

         final ArrayList<T> result = new ArrayList<>();
         final RSet rset = new RSet(this.s.executeQuery());
         try {
            while (rset.next()) {
               result.add(mapper.apply(rset));
            }
         } finally {
            rset.close();
            this.s.close();
         }
         return result;
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public void forEachQuery(final RSetConsumer c) throws SQLException {

         final RSet rset = new RSet(this.s.executeQuery());
         try {
            while (rset.next()) {
               c.accept(rset);
            }
         } finally {
            rset.close();
            this.s.close();
         }
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public <T> T foldQuery(final T seed, final RSetBiFunction<T> reducer)
              throws SQLException {

         T accumulator = seed;
         final RSet rset = new RSet(this.s.executeQuery());
         try {
            while (rset.next()) {
               accumulator = reducer.apply(accumulator, rset);
            }
         } finally {
            rset.close();
            this.s.close();
         }
         return accumulator;
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
   public final static class RSet {

      /***********************************************************************
       * 
       **********************************************************************/
      private RSet(final ResultSet rs) {

         this.rs = rs;
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public byte getByte() throws SQLException {

         return this.rs.getByte(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public short getShort() throws SQLException {

         return this.rs.getShort(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public int getInt() throws SQLException {

         return this.rs.getInt(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public long getLong() throws SQLException {

         return this.rs.getLong(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public float getFloat() throws SQLException {

         return this.rs.getFloat(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public double getDouble() throws SQLException {

         return this.rs.getDouble(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public String getString() throws SQLException {

         return this.rs.getString(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public Date getDate() throws SQLException {

         return this.rs.getDate(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public boolean getBoolean() throws SQLException {

         return this.rs.getBoolean(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public Object getObject() throws SQLException {

         return this.rs.getObject(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public byte[] getBytes() throws SQLException {

         return this.rs.getBytes(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public Time getTime() throws SQLException {

         return this.rs.getTime(++this.columnIndex);
      }

      /***********************************************************************
       * 
       **********************************************************************/
      public Timestamp getTimestamp() throws SQLException {

         return this.rs.getTimestamp(++this.columnIndex);
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public InputStream getAsciiStream() throws SQLException {

         return this.rs.getAsciiStream(++this.columnIndex);
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public InputStream getBinaryStream() throws SQLException {

         return this.rs.getBinaryStream(++this.columnIndex);
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public BigDecimal getBigDecimal() throws SQLException {

         return this.rs.getBigDecimal(++this.columnIndex);
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public SQLWarning getWarnings() throws SQLException {

         return this.rs.getWarnings();
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public void clearWarnings() throws SQLException {

         this.rs.clearWarnings();
      }

      /***********************************************************************
       * 
       **********************************************************************/
      private boolean next() throws SQLException {

         this.columnIndex = 0;
         return this.rs.next();
      }

      /***********************************************************************
       * 
       **********************************************************************/
      private void close() throws SQLException {

         this.rs.close();
      }

      /***********************************************************************
       * 
       **********************************************************************/
      private boolean wasNull() throws SQLException {

         return this.rs.wasNull();
      }
      /***********************************************************************
       * 
       **********************************************************************/
      private final ResultSet rs;
      private int columnIndex = 0;
   }

   /***************************************************************************
    * 
    **************************************************************************/
   @FunctionalInterface
   public interface RSetFunction<R> {

      /***********************************************************************
       * 
       **********************************************************************/
      R apply(RSet rs) throws SQLException;
   }

   /***************************************************************************
    * 
    **************************************************************************/
   @FunctionalInterface
   public interface RSetConsumer {

      /***********************************************************************
       * 
       **********************************************************************/
      void accept(RSet rs) throws SQLException;
   }

   /***************************************************************************
    * 
    **************************************************************************/
   @FunctionalInterface
   public interface RSetBiFunction<T> {

      /***********************************************************************
       * 
       **********************************************************************/
      T apply(T seed, RSet rs) throws SQLException;
   }
}
