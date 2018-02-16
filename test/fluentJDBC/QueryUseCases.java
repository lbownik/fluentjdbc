package fluentJDBC;

import static fluentJDBC.FluentConnection.using;
import fluentJDBC.fakes.FakeConnection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/*******************************************************************************
 *
 * @author
 ******************************************************************************/
public class QueryUseCases {

   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void forEach_CallsConsumer_ForProperInvocation()
           throws Exception {

      AtomicBoolean consumerCalled = new AtomicBoolean(false);

      using(this.c).prepare("sql").andForEach(rs -> consumerCalled.getAndSet(true));

      assertProperQueryInvariants();
      assertTrue(consumerCalled.get());
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void forEach_DoesNotCallsConsumer_IfNoValuesAreFound()
           throws Exception {

      AtomicBoolean consumerCalled = new AtomicBoolean(false);
      this.c.s.rs.nextCalled = true;

      using(this.c).prepare("sql").andForEach(rs -> consumerCalled.getAndSet(true));

      assertProperQueryInvariants();
      assertFalse(consumerCalled.get());
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void forEach_ClosesResoruces_IfConsumerThrowsException()
           throws Exception {

      try {
         using(this.c).prepare("sql").andForEach(rs -> {
            throw new Exception();
         });
         fail();
      } catch (Exception e) {
         assertProperQueryInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void forEach_ClosesReqources_IfExecuteQuerryThrowsException()
           throws Exception {

      this.c.s.executeQueryThrowsException = true;
      try {
         using(this.c).prepare("sql").andForEach(rs -> {
         });
         fail();
      } catch (Exception e) {
         assertFailedQueryInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void forEach_throwsException_ForNullConsumer()
           throws Exception {

      try {
         using(this.c).prepare("sql").andForEach(null);
         fail();
      } catch (NullPointerException e) {
         assertProperQueryInvariants();
      } catch (Exception e) {
         fail();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMap_CallsMapperAndReturnsResult_ForProperInvocation()
           throws Exception {

      AtomicBoolean mapperCalled = new AtomicBoolean(false);

      List<String> result = using(this.c).prepare("sql").
              andMap(rs -> {
                 mapperCalled.getAndSet(true);
                 return "result";
              });

      assertProperQueryInvariants();
      assertTrue(mapperCalled.get());
      assertEquals(1, result.size());
      assertEquals("result", result.get(0));
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMap_DoesNotCallMapperAndReturnsEmptyResult_IfNoValuesAreFound()
           throws Exception {

      AtomicBoolean mapperCalled = new AtomicBoolean(false);
      this.c.s.rs.nextCalled = true;

      List<String> result = using(this.c).prepare("sql").
              andMap(rs -> {
                 mapperCalled.getAndSet(true);
                 return "result";
              });

      assertProperQueryInvariants();
      assertFalse(mapperCalled.get());
      assertEquals(0, result.size());
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMap_ThrowsException_ForNullMapper()
           throws Exception {

      try {
         using(this.c).prepare("sql").andMap(null);
         fail();
      } catch (NullPointerException e) {
         assertProperQueryInvariants();
      } catch (Exception e) {
         fail();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMap_ClosesResources_IfMapperThrowsException()
           throws Exception {

      try {
         using(this.c).prepare("sql").andMap(rs -> {
            throw new Exception();
         });
         fail();
      } catch (Exception e) {
         assertProperQueryInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMap_ClosesResources_IfExecuteQuerryThrowsException()
           throws Exception {

      this.c.s.executeQueryThrowsException = true;
      try {
         using(this.c).prepare("sql").andMap(rs -> "");
         fail();
      } catch (Exception e) {
         assertFailedQueryInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMapOne_ClosesResources_ForProperInvocation() throws Exception {

      AtomicBoolean mapperCalled = new AtomicBoolean(false);

      Optional<String> result = using(this.c).prepare("sql").
              andMapOne(rs -> {
                 mapperCalled.getAndSet(true);
                 return "result";
              });

      assertProperQueryInvariants();
      assertTrue(mapperCalled.getAndSet(true));
      assertTrue(result.isPresent());
      assertEquals("result", result.get());
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMapOne_DoesNotCallMapper_IfNoValueIsFound()
           throws Exception {

      AtomicBoolean mapperCalled = new AtomicBoolean(false);
      this.c.s.rs.nextCalled = true;

      Optional<String> result = using(this.c).prepare("sql").
              andMapOne(rs -> {
                 mapperCalled.getAndSet(true);
                 return "result";
              });

      assertProperQueryInvariants();
      assertFalse(mapperCalled.getAndSet(true));
      assertFalse(result.isPresent());
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMapOne_ThrowsException_ForNullMapper()
           throws Exception {

      try {
         using(this.c).prepare("sql").andMapOne(null);
         fail();
      } catch (NullPointerException e) {
         assertProperQueryInvariants();
      } catch (Exception e) {
         fail();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMapOne_ClosesResources_IfMapperThrowsException()
           throws Exception {

      try {
         using(this.c).prepare("sql").andMapOne(rs -> {
            throw new Exception();
         });
         fail();
      } catch (Exception e) {
         assertProperQueryInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andMapOne_ClosesResources_IfExecuteQuerryThrowsException()
           throws Exception {

      this.c.s.executeQueryThrowsException = true;
      try {
         using(this.c).prepare("sql").andMapOne(rs -> "");
         fail();
      } catch (Exception e) {
         assertFailedQueryInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andReduce_CallsReducerAndReturnsResult_ForProperInvocation()
           throws Exception {

      StringBuilder result = using(this.c).prepare("sql").
              andReduce(new StringBuilder(),
                      (acc, rs) -> {
                         acc.append("result");
                         return acc;
                      });

      assertProperQueryInvariants();
      assertEquals("result", result.toString());
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andReduce_DoesNotCallsReducerAndReturnsSeed_IfNoValuesAreFound()
           throws Exception {

      AtomicBoolean reducerCalled = new AtomicBoolean(false);
      this.c.s.rs.nextCalled = true;

      StringBuilder result = using(this.c).prepare("sql").
              andReduce(new StringBuilder(),
                      (acc, rs) -> {
                         reducerCalled.getAndSet(true);
                         acc.append("result");
                         return acc;
                      });

      assertProperQueryInvariants();
      assertFalse(reducerCalled.get());
      assertEquals("", result.toString());
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andReduce_ThrowsException_ForNullReducer()
           throws Exception {

      try {
         using(this.c).prepare("sql").andReduce(new StringBuilder(), null);
         fail();
      } catch (NullPointerException e) {
         assertProperQueryInvariants();
      } catch (Exception e) {
         fail();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andReduce_CallsReducer_ForNullSeed()
           throws Exception {

      AtomicBoolean reducerCalled = new AtomicBoolean(false);

      String result = using(this.c).prepare("sql").andReduce((String) null,
              (acc, rs) -> {
                 reducerCalled.getAndSet(true);
                 assertNull(acc);
                 return acc;
              });

      assertProperQueryInvariants();
      assertTrue(reducerCalled.get());
      assertNull(result);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andReduce_ClosesResources_IfReducerThrowsException()
           throws Exception {

      try {
         using(this.c).prepare("sql").andReduce(new StringBuilder(),
                 (acc, rs) -> {
                    throw new Exception();
                 });
         fail();
      } catch (Exception e) {
         assertProperQueryInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andReduce_ClosesResources_IfExecuteQueryThrowsException()
           throws Exception {

      this.c.s.executeQueryThrowsException = true;
      try {
         using(this.c).prepare("sql").andReduce(new StringBuilder(),
                 (acc, rs) -> acc);
         fail();
      } catch (Exception e) {
         assertFailedQueryInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private void assertProperQueryInvariants() {

      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeQueryCalled);
      assertTrue(this.c.s.rs.nextCalled);
      assertTrue(this.c.s.rs.isClosed);
      assertTrue(this.c.s.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private void assertFailedQueryInvariants() {

      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeQueryCalled);
      assertFalse(this.c.s.rs.nextCalled);
      assertTrue(this.c.s.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private final FakeConnection c = new FakeConnection();
}
