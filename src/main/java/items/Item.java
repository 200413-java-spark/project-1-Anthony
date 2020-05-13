package items;

import java.sql.Date;

public class Item {
    public String league;
    public Date date;
    public int itemID;
    public String name;
    public double value;
    public String confidence;

    public Item(String league, Date date, int itemID, String name, double value, String confidence){
        this.league = league;
        this.date = date;
        this.itemID = itemID;
        this.name = name;
        this.value = value;
        this.confidence = confidence;
    }
}
