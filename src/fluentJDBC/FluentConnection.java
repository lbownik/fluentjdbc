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
import static java.sql.Statement.RETURN_GENERATED_KEYS;
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
public final class FluentConnection {
   /***************************************************************************
    * 
    **************************************************************************/
   public static FluentConnection using(final Connection c) {

      if (c == null) {
         throw new NullPointerException("Null connection.");
      }
      return new FluentConnection(c);
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
   public Preparator prepareWithKey(final String sql)
           throws SQLException {

      return new Preparator(this.c.prepareStatement(sql,RETURN_GENERATED_KEYS));
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
   private FluentConnection(final Connection c) {

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

         this.statement = s;
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final byte v) throws SQLException {

         return set((s, index) -> s.setByte(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final short v) throws SQLException {

         return set((s, index) -> s.setShort(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final int v) throws SQLException {

         return set((s, index) -> s.setInt(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final long v) throws SQLException {

         return set((s, index) -> s.setLong(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final float v) throws SQLException {

         return set((s, index) -> s.setFloat(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final double v) throws SQLException {

         return set((s, index) -> s.setDouble(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final String v) throws SQLException {

         return set((s, index) -> s.setString(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Date v) throws SQLException {

         return set((s, index) -> s.setDate(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final boolean v) throws SQLException {

         return set((s, index) -> s.setBoolean(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final BigDecimal v) throws SQLException {

         return set((s, index) -> s.setBigDecimal(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final byte[] v) throws SQLException {

         return set((s, index) -> s.setBytes(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Time v) throws SQLException {

         return set((s, index) -> s.setTime(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Time v, final Calendar c)
              throws SQLException {

         return set((s, index) -> s.setTime(index, v, c));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Timestamp v) throws SQLException {

         return set((s, index) -> s.setTimestamp(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Object v) throws SQLException {

         return set((s, index) -> s.setObject(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Blob v) throws SQLException {

         return set((s, index) -> s.setBlob(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Clob v) throws SQLException {

         return set((s, index) -> s.setClob(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final NClob v) throws SQLException {

         return set((s, index) -> s.setNClob(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Array v) throws SQLException {

         return set((s, index) -> s.setArray(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Reader v) throws SQLException {

         return set((s, index) -> s.setCharacterStream(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final InputStream v) throws SQLException {

         return set((s, index) -> s.setBinaryStream(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final URL v) throws SQLException {

         return set((s, index) -> s.setURL(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final RowId v) throws SQLException {

         return set((s, index) -> s.setRowId(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final SQLXML v) throws SQLException {

         return set((s, index) -> s.setSQLXML(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Ref v) throws SQLException {

         return set((s, index) -> s.setRef(index, v));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator set(final Setter setter) throws SQLException {

         return apply((s) -> setter.set(s, ++this.paramIndex));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Preparator apply(final Consumer<PreparedStatement, SQLException> consumer)
              throws SQLException {

         try {
            consumer.consume(this.statement);
            return this;
         } catch (final Exception e) {
            this.statement.close();
            throw e;
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public int andUpdate() throws SQLException {

         try {
            return this.statement.executeUpdate();
         } finally {
            this.statement.close();
         }
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public Object andUpdateReturningKey() throws SQLException {

         try {
            this.statement.executeUpdate();
            try (final ResultSet rs = this.statement.getGeneratedKeys()) {
               rs.next();
               return rs.getObject(1);
            }
         } finally {
            this.statement.close();
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

         return andReduce(new ArrayList<>(size), 
                 (list, rs) -> { list.add(mapper.map(rs)); return list;});
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public <T> Optional<T> andMapOne(final Mapper<T> mapper)
              throws Exception {

         return andReduce(Optional.empty(), (s, rs) -> Optional.of(mapper.map(rs)));
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public void andForEach(final Consumer<ResultSet, Exception> consumer)
              throws Exception {

         andReduce(null, (seed, rs) -> {consumer.consume(rs); return null;});
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public <T> T andReduce(T accumulator, final Reducer<T> reducer)
              throws Exception {

         try (final ResultSet rs = this.statement.executeQuery()) {
            while (rs.next()) {
               accumulator = reducer.reduce(accumulator, rs);
            }
         } finally {
            this.statement.close();
         }
         return accumulator;
      }
      /***********************************************************************
       * 
       **********************************************************************/
      public void close() throws Exception {

         this.statement.close();
      }
      /***********************************************************************
       * 
       **********************************************************************/
      private final PreparedStatement statement;
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
      void set(PreparedStatement s, int index) throws SQLException;
   }
}