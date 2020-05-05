package spark;

public class HelloGreeter implements Greetable{
    @Override
    public String greet(String name) {
        return "Hello, " + name;
    }
}