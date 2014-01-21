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
//IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, ///HETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE 
//SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package example;

import static fluentJDBC.FluentJDBC.*; //Note static miport
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/*******************************************************************************
 *
 * @author lukasz.bownik@gmail.com
 ******************************************************************************/
public class Example {
    
   /***************************************************************************
    * 
    **************************************************************************/
    static class User {
        
        public User(final String name, final String pass) {
            
            this.name =name;
            this.pass = pass;
        }
        public String name;
        public String pass;
    }
    /***************************************************************************
    * 
    **************************************************************************/
    public static void main(String[] args) throws Exception {
        
        Connection c = null;
        
        //classic JDBC
        List<User> list = new ArrayList<>();
        try(final PreparedStatement s = 
                c.prepareStatement("select name, password from user where name = ?")){
           s.setString(1, "adam");
           try(final ResultSet rs = s.executeQuery()) {
              list.add(new User(rs.getString(1), rs.getString(2)));
           }
           
        }
          
        //FluentJDBC - list retireval
        List<User> list2 = using(c).prepare("select name, password from user where name = ?").
                set("adam").mapQuery( rs -> new User(rs.getString(), rs.getString()));
        
        //FluentJDBC - single value retrieval
        String s = using(c).prepare("select name, password from user where name = ?").
                set("adam").foldQuery("", (seed, rs) -> rs.getString());
        
        //FluentJDBC - result printing
        using(c).prepare("select name, password from user where name = ?").
                set("adam").forEachQuery( rs -> System.out.println(rs.getString()));
    }
}
