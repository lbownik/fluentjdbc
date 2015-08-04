#A fluent JDBC wrapper with lambdas.

Those who are not convinced to use Hibernate to manage persistence are forced to use plain old JDBC API. Though powerfull it requires lot of typing to get it right. For example retrieving user data from database often requires such code snippet:
```
final List<User> result = new ArrayList<>();
final PreparedStatement s = 
        conn.prepareStatement("select name, password from user where name = ?");
try{
   s.setString(1, "adam");
   final ResultSet rs = s.executeQuery();
   try {
      result.add(new User(rs.getString(1), rs.getString(2)));
   } finally {
      rs.close();
   }
} finally {
   s.close();
}
```
With Java 7’s try-with-resources this code is a little shorter:
```
final List<User> result = new ArrayList<>();
try(final PreparedStatement s = 
        conn.prepareStatement("select name, password from user where name = ?")){
   s.setString(1, "adam");
   try(final ResultSet rs = s.executeQuery()) {
      result.add(new User(rs.getString(1), rs.getString(2)));
   }
}
```
With Java 8’s lambda expressions and FluentJDBC wrapper this code can be delimited even further to something like this.
```
final List<User> l = using(conn).prepare("select name,password from user where name = ?").
      set("adam").mapQuery( rs -> new User(rs.getString(), rs.getString()));
```
The wrapper uses static imports (in form of `import static fluentJDBC.FluentJDBC.*;`), *fluent method chaining*,  with *implicit parameter index incrementation* and *functional interfaces* which allow lambda expression as parameters to mapQuery, foldQuery and forEachQuery methods.
Other examples of FluentJDBC uage:
```
//FluentJDBC - single value retrieval
final String s = using(conn).prepare("select name, password from user where name = ?").
      set("adam").foldQuery("", (seed, rs) -> rs.getString());
        
//FluentJDBC - result printing
using(conn).prepare("select name, password from user where name = ?").
   set("adam").forEachQuery( rs -> System.out.println(rs.getString()));
```
FluentJDBC is a single file, lightweight wrapper that You can copy to Your project and use it. It is by no means a finished product, but I encourage You to extend it to meet Your own needs.

