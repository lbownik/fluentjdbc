package fluentJDBC;

import static fluentJDBC.FluentConnection.using;
import fluentJDBC.fakes.FakeConnection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/*******************************************************************************
 *
 * @author
 ******************************************************************************/
public class ManipulationUseCases {
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void setInt_PassesArgumentsToStatement_ForProperInvocation()
           throws Exception {

      using(this.c).prepare("sql").set(13).set(14);

      assertEquals(2, this.c.s.arguments.size());
      assertEquals(13, this.c.s.arguments.get(1));
      assertEquals(14, this.c.s.arguments.get(2));
      assertProperInvocationInvariants();
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void setInt_ClosesResources_IfSetThrowsException()
           throws Exception {

      this.c.s.setIntThrowsException = true;
      try {
         using(this.c).prepare("sql").set(13);
         fail();
      } catch (Exception e) {
         assertEquals(1, this.c.s.arguments.size());
         assertEquals(13, this.c.s.arguments.get(1));
         assertFailedInvocationInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void setWithSetter_PassesArgumentToStatement_ForProperInvocation()
           throws Exception {

      using(this.c).prepare("sql").set((s, index) -> s.setInt(index, 13));

      assertEquals(1, this.c.s.arguments.size());
      assertEquals(13, this.c.s.arguments.get(1));
      assertProperInvocationInvariants();
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void setWithSetter_ClosesResources_ForNullSetter()
           throws Exception {

      try {
         using(this.c).prepare("sql").set((FluentConnection.Setter) null);
         fail();
      } catch (NullPointerException e) {
         assertFailedInvocationInvariants();
      } catch (Exception e) {
         fail();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void tsetWithSetter_ClosesResources_IfSetterThrowsException()
           throws Exception {

      try {
         using(this.c).prepare("sql").set((s, index) -> {
            throw new SQLException();
         });
         fail();
      } catch (Exception e) {
         assertEquals(0, this.c.s.arguments.size());
         assertFailedInvocationInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void apply_CallsConsumer_ForProperInvocation()
           throws Exception {

      final AtomicBoolean consumerCalled = new AtomicBoolean();

      using(this.c).prepare("sql").apply((s) -> consumerCalled.getAndSet(true));

      assertTrue(consumerCalled.get());
      assertEquals(0, this.c.s.arguments.size());
      assertProperInvocationInvariants();
   }
   /***************************************************************************
    * 
    **************************************************************************/
   @Test
   public void apply_ClosesResources_IfConsumerThrowsException()
           throws Exception {

      try {
         using(this.c).prepare("sql").apply((s) -> {
            throw new SQLException();
         });
         fail();
      } catch (SQLException e) {
         assertEquals(0, this.c.s.arguments.size());
         assertFailedInvocationInvariants();
      }
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private void assertProperInvocationInvariants() {

      assertEquals("sql", this.c.s.sql);
      assertFalse(this.c.s.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private void assertFailedInvocationInvariants() {

      assertEquals("sql", this.c.s.sql);
      assertTrue(this.c.s.isClosed);
   }
   /***************************************************************************
    * 
    **************************************************************************/
   private final FakeConnection c = new FakeConnection();
}
