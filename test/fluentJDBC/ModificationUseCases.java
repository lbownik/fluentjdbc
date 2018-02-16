package fluentJDBC;

import static fluentJDBC.FluentConnection.using;
import fluentJDBC.fakes.FakeConnection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/*******************************************************************************
 *
 * @author
 ******************************************************************************/
public class ModificationUseCases {
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andUpdate_ReturnsNumberOfRows_ForProperInvocation()
           throws Exception {

      int result = using(this.c).prepare("sql").andUpdate();

      assertEquals(1, result);
      assertUpdateInvariants();
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andUpdate_ClosesResources_IfExecuteUpdateThrowsException() 
           throws Exception {

      this.c.s.executeUpdateThrowsException = true;

      try {
         using(this.c).prepare("sql").andUpdate();
         fail();
      } catch (Exception e) {
         assertUpdateInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andUpdateReturningKey_ReturnGeneratedKey_ForProperInvocation()
           throws Exception {

      Object key = using(this.c).prepare("sql", true).andUpdateReturningKey();

      assertUpdateInvariants();
      assertEquals("key1", key);
      assertTrue(this.c.s.generatedKeys.nextCalled);
      assertTrue(this.c.s.generatedKeys.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andUpdateReturningKey_ClosesResources_IfExecuteUpdateThrowsException()
           throws Exception {

      this.c.s.executeUpdateThrowsException = true;

      try {
         using(this.c).prepare("sql", true).andUpdateReturningKey();
         fail();
      } catch (Exception e) {
         assertUpdateInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andUpdateReturningKey_ClosesResources_IfGetGeneratedKeysThrowsException()
           throws Exception {

      this.c.s.getGeneratedKeysThrowsException = true;

      try {
         using(this.c).prepare("sql", true).andUpdateReturningKey();
         fail();
      } catch (Exception e) {
         assertUpdateInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void andUpdateReturningKey_ClosesReources_IfGetObjectThrowsException()
           throws Exception {

      this.c.s.generatedKeys.getObjectThrowsException = true;

      try {
         using(this.c).prepare("sql", true).andUpdateReturningKey();
         fail();
      } catch (Exception e) {
         assertUpdateInvariants();
         assertTrue(this.c.s.generatedKeys.nextCalled);
         assertTrue(this.c.s.generatedKeys.isClosed);
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private void assertUpdateInvariants() {

      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.executeUpdateCalled);
      assertTrue(this.c.s.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private final FakeConnection c = new FakeConnection();
}
