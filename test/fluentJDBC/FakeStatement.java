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

import com.sun.jmx.remote.util.OrderClassLoaders;
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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;

/*******************************************************************************
 *
 * @author lukasz.bownik@gmail.com
 ******************************************************************************/
public class FakeStatement implements PreparedStatement {

   public FakeResultSet rs = new FakeResultSet();
   public FakeResultSet generatedKeys = new FakeResultSet();
   public boolean getGeneratedKeysThrowsException = false;
   public String sql = null;
   public HashMap<Integer, Object> arguments = new HashMap<>();
   public boolean executeQueryCalled = false;
   public boolean executeUpdateCalled = false;
   public boolean isClosed = false;
   public boolean executeUpdateThrowsException = false;
   public boolean executeQueryThrowsException = false;
   public boolean setIntThrowsException = false;

   @Override
   public ResultSet executeQuery() throws SQLException {

      this.executeQueryCalled = true;
      if (this.executeQueryThrowsException) {
         throw new SQLException();
      }
      return this.rs;
   }
   @Override
   public int executeUpdate() throws SQLException {

      this.executeUpdateCalled = true;
      if (this.executeUpdateThrowsException) {
         throw new SQLException();
      }
      return 1;
   }
   @Override
   public void setNull(int parameterIndex, int sqlType) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setBoolean(int parameterIndex, boolean x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setByte(int parameterIndex, byte x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setShort(int parameterIndex, short x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setInt(int parameterIndex, int x) throws SQLException {

      this.arguments.put(parameterIndex, x);
      if(this.setIntThrowsException) {
         throw new SQLException();
      }
   }
   @Override
   public void setLong(int parameterIndex, long x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setFloat(int parameterIndex, float x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setDouble(int parameterIndex, double x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setString(int parameterIndex, String x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setBytes(int parameterIndex, byte[] x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setDate(int parameterIndex, Date x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setTime(int parameterIndex, Time x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void clearParameters() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public void setObject(int parameterIndex, Object x) throws SQLException {

      this.arguments.put(parameterIndex, x);
   }
   @Override
   public boolean execute() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void addBatch() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setRef(int parameterIndex, Ref x) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setBlob(int parameterIndex, Blob x) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setClob(int parameterIndex, Clob x) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setArray(int parameterIndex, Array x) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public ResultSetMetaData getMetaData() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setURL(int parameterIndex, URL x) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public ParameterMetaData getParameterMetaData() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setRowId(int parameterIndex, RowId x) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setNString(int parameterIndex, String value) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setNClob(int parameterIndex, NClob value) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setClob(int parameterIndex, Reader reader) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setNClob(int parameterIndex, Reader reader) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public ResultSet executeQuery(String sql) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int executeUpdate(String sql) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void close() throws SQLException {

      this.isClosed = true;
   }
   @Override
   public int getMaxFieldSize() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setMaxFieldSize(int max) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int getMaxRows() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setMaxRows(int max) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setEscapeProcessing(boolean enable) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int getQueryTimeout() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setQueryTimeout(int seconds) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void cancel() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public SQLWarning getWarnings() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void clearWarnings() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setCursorName(String name) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean execute(String sql) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public ResultSet getResultSet() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int getUpdateCount() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean getMoreResults() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setFetchDirection(int direction) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int getFetchDirection() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setFetchSize(int rows) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int getFetchSize() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int getResultSetConcurrency() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int getResultSetType() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void addBatch(String sql) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void clearBatch() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int[] executeBatch() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public Connection getConnection() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean getMoreResults(int current) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public ResultSet getGeneratedKeys() throws SQLException {
      
      if(this.getGeneratedKeysThrowsException) {
         throw new SQLException();
      }
      return this.generatedKeys;
   }
   @Override
   public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int executeUpdate(String sql, String[] columnNames) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean execute(String sql, int[] columnIndexes) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean execute(String sql, String[] columnNames) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public int getResultSetHoldability() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean isClosed() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void setPoolable(boolean poolable) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean isPoolable() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public void closeOnCompletion() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean isCloseOnCompletion() throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }
   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      throw new UnsupportedOperationException("Not supported yet."); 
   }

}
