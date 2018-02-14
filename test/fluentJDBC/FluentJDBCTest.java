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

import org.junit.Test;
import static fluentJDBC.FluentJDBC.using;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.Assert.*;

/*******************************************************************************
 *
 * @author lukasz.bownik@gmail.com
 ******************************************************************************/
public class FluentJDBCTest {

   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_using_nullArgument_throwsException() throws Exception {

      try {
         using(null);
         fail();
      } catch (NullPointerException e) {
         assertTrue(true);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andUpdate_proper_returnNumberOfRows() throws Exception {

      int result = using(this.c).prepare("sql").andUpdate();

      assertEquals(1, result);
      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeUpdateCalled);
      assertTrue(this.c.s.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andUpdate_proper_updateThrowsException() throws Exception {

      this.c.s.executeUpdateThrowsException = true;

      try {
         using(this.c).prepare("sql").andUpdate();
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeUpdateCalled);
         assertTrue(this.c.s.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andUpdateReturningKey_proper_returnGeneratedKey()
           throws Exception {

      Object key = using(this.c).prepare("sql", true).andUpdateReturningKey();

      assertEquals("key1", key);
      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeUpdateCalled);
      assertTrue(this.c.s.isClosed);
      assertTrue(this.c.s.generatedKeys.nextCalled);
      assertTrue(this.c.s.generatedKeys.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andUpdateReturningKey_proper_updateThrowsException()
           throws Exception {

      this.c.s.executeUpdateThrowsException = true;

      try {
         using(this.c).prepare("sql", true).andUpdateReturningKey();
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeUpdateCalled);
         assertTrue(this.c.s.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andUpdateReturningKey_proper_getGeneratedKeysThrowsException()
           throws Exception {

      this.c.s.getGeneratedKeysThrowsException = true;

      try {
         using(this.c).prepare("sql", true).andUpdateReturningKey();
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeUpdateCalled);
         assertTrue(this.c.s.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andUpdateReturningKey_proper_getObjectThrowsException()
           throws Exception {

      this.c.s.generatedKeys.getObjectThrowsException = true;

      try {
         using(this.c).prepare("sql", true).andUpdateReturningKey();
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeUpdateCalled);
         assertTrue(this.c.s.isClosed);
         assertTrue(this.c.s.generatedKeys.nextCalled);
         assertTrue(this.c.s.generatedKeys.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_forEach_proper_consumerCalled() throws Exception {

      AtomicBoolean consumerCalled = new AtomicBoolean(false);

      using(this.c).prepare("sql").andForEach(rs -> consumerCalled.getAndSet(true));

      assertTrue(consumerCalled.getAndSet(true));
      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeQueryCalled);
      assertTrue(this.c.s.isClosed);
      assertTrue(this.c.s.rs.nextCalled);
      assertTrue(this.c.s.rs.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_forEach_proper_consumerThrowsException() throws Exception {

      try {
         using(this.c).prepare("sql").andForEach(rs -> {
            throw new Exception();
         });
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertTrue(this.c.s.rs.nextCalled);
         assertTrue(this.c.s.rs.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_forEach_proper_executeQuerryThrowsException() throws Exception {

      this.c.s.executeQueryThrowsException = true;
      try {
         using(this.c).prepare("sql").andForEach(rs -> {
         });
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertFalse(this.c.s.rs.nextCalled);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_forEach_nullConsumer_throwsException() throws Exception {

      try {
         using(this.c).prepare("sql").andForEach(null);
         fail();
      } catch (NullPointerException e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertTrue(this.c.s.rs.nextCalled);
         assertTrue(this.c.s.rs.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andMap_proper_mapperCalled() throws Exception {

      AtomicBoolean mapperCalled = new AtomicBoolean(false);

      List<String> result = using(this.c).prepare("sql").
              andMap(rs -> {
                 mapperCalled.getAndSet(true);
                 return "result";
              });

      assertTrue(mapperCalled.getAndSet(true));
      assertEquals(1, result.size());
      assertEquals("result", result.get(0));
      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeQueryCalled);
      assertTrue(this.c.s.isClosed);
      assertTrue(this.c.s.rs.nextCalled);
      assertTrue(this.c.s.rs.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andMap_nullMapper_throwsException() throws Exception {

      try {
         using(this.c).prepare("sql").andMap(null);
         fail();
      } catch (NullPointerException e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertTrue(this.c.s.rs.nextCalled);
         assertTrue(this.c.s.rs.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andMap_proper_mapperThrowsException() throws Exception {

      try {
         using(this.c).prepare("sql").andMap(rs -> {
            throw new Exception();
         });
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertTrue(this.c.s.rs.nextCalled);
         assertTrue(this.c.s.rs.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andMap_proper_executeQuerryThrowsException() throws Exception {

      this.c.s.executeQueryThrowsException = true;
      try {
         using(this.c).prepare("sql").andMap(rs -> "");
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertFalse(this.c.s.rs.nextCalled);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andMapOne_proper_mapperCalled() throws Exception {

      AtomicBoolean mapperCalled = new AtomicBoolean(false);

      Optional<String> result = using(this.c).prepare("sql").
              andMapOne(rs -> {
                 mapperCalled.getAndSet(true);
                 return "result";
              });

      assertTrue(mapperCalled.getAndSet(true));
      assertTrue(result.isPresent());
      assertEquals("result", result.get());
      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeQueryCalled);
      assertTrue(this.c.s.isClosed);
      assertTrue(this.c.s.rs.nextCalled);
      assertTrue(this.c.s.rs.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andMapOne_noValue_mapperNotCalled() throws Exception {

      AtomicBoolean mapperCalled = new AtomicBoolean(false);
      this.c.s.rs.nextCalled = true;

      Optional<String> result = using(this.c).prepare("sql").
              andMapOne(rs -> {
                 mapperCalled.getAndSet(true);
                 return "result";
              });

      assertFalse(mapperCalled.getAndSet(true));
      assertFalse(result.isPresent());
      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeQueryCalled);
      assertTrue(this.c.s.isClosed);
      assertTrue(this.c.s.rs.nextCalled);
      assertTrue(this.c.s.rs.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andMapOne_nullMapper_throwsException() throws Exception {

      try {
         using(this.c).prepare("sql").andMapOne(null);
         fail();
      } catch (NullPointerException e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertTrue(this.c.s.rs.nextCalled);
         assertTrue(this.c.s.rs.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andMapOne_proper_mapperThrowsException() throws Exception {

      try {
         using(this.c).prepare("sql").andMapOne(rs -> {
            throw new Exception();
         });
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertTrue(this.c.s.rs.nextCalled);
         assertTrue(this.c.s.rs.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andMapOne_proper_executeQuerryThrowsException() throws Exception {

      this.c.s.executeQueryThrowsException = true;
      try {
         using(this.c).prepare("sql").andMapOne(rs -> "");
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertFalse(this.c.s.rs.nextCalled);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andReduce_proper_reducerCalled() throws Exception {

      StringBuilder result = using(this.c).prepare("sql").
              andReduce(new StringBuilder(),
                      (acc, rs) -> {
                         acc.append("result");
                         return acc;
                      });

      assertEquals("result", result.toString());
      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeQueryCalled);
      assertTrue(this.c.s.isClosed);
      assertTrue(this.c.s.rs.nextCalled);
      assertTrue(this.c.s.rs.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andReduce_nullReducer_throwsException() throws Exception {

      try {
         using(this.c).prepare("sql").andReduce(new StringBuilder(), null);
         fail();
      } catch (NullPointerException e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertTrue(this.c.s.rs.nextCalled);
         assertTrue(this.c.s.rs.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andReduce_nullSeed_reducerCalled() throws Exception {

      AtomicBoolean reducerCalled = new AtomicBoolean(false);

      String result = using(this.c).prepare("sql").andReduce((String) null,
              (acc, rs) -> {
                 reducerCalled.getAndSet(true);
                 assertNull(acc);
                 return acc;
              });

      assertTrue(reducerCalled.get());
      assertNull(result);
      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeQueryCalled);
      assertTrue(this.c.s.isClosed);
      assertTrue(this.c.s.rs.nextCalled);
      assertTrue(this.c.s.rs.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andReduce_proper_reducerThrowsException() throws Exception {

      try {
         using(this.c).prepare("sql").andReduce(new StringBuilder(),
                 (acc, rs) -> {
                    throw new Exception();
                 });
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertTrue(this.c.s.rs.nextCalled);
         assertTrue(this.c.s.rs.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_andReduce_proper_executeQuerryThrowsException() throws Exception {

      this.c.s.executeQueryThrowsException = true;
      try {
         using(this.c).prepare("sql").andReduce(new StringBuilder(),
                 (acc, rs) -> acc);
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertTrue(this.c.s.executeQueryCalled);
         assertTrue(this.c.s.isClosed);
         assertFalse(this.c.s.rs.nextCalled);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_setInt_proper_valueSet() throws Exception {

      using(this.c).prepare("sql").set(13);

      assertEquals("sql", this.c.s.sql);
      assertEquals(1, this.c.s.arguments.size());
      assertEquals(13, this.c.s.arguments.get(1));
      assertFalse(this.c.s.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_setInt_proper_setThrowsException() throws Exception {

      this.c.s.setIntThrowsException = true;
      try {
         using(this.c).prepare("sql").set(13);
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertEquals(1, this.c.s.arguments.size());
         assertEquals(13, this.c.s.arguments.get(1));
         assertTrue(this.c.s.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_setWithSetter_proper_valueSet() throws Exception {

      using(this.c).prepare("sql").set((s, index) -> s.setInt(index, 13));

      assertEquals("sql", this.c.s.sql);
      assertEquals(1, this.c.s.arguments.size());
      assertEquals(13, this.c.s.arguments.get(1));
      assertFalse(this.c.s.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_setWithSetter_proper_setterThrowsException() throws Exception {

      try {
         using(this.c).prepare("sql").set((s, index) -> {throw new SQLException();});
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertEquals(0, this.c.s.arguments.size());
         assertTrue(this.c.s.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_apply_proper_consumerThrowsException() throws Exception {

      try {
         using(this.c).prepare("sql").apply((s) -> {throw new SQLException();});
         fail();
      } catch (Exception e) {
         assertEquals("sql", this.c.s.sql);
         assertEquals(0, this.c.s.arguments.size());
         assertTrue(this.c.s.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void test_apply_proper_consumerCalled() throws Exception {

      final AtomicBoolean consumerCalled = new AtomicBoolean();
      try {
         using(this.c).prepare("sql").apply((s) -> consumerCalled.getAndSet(true));
      } catch (Exception e) {
         assertTrue(consumerCalled.get());
         assertEquals("sql", this.c.s.sql);
         assertEquals(0, this.c.s.arguments.size());
         assertTrue(this.c.s.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private final FakeConnection c = new FakeConnection();
}
