package items;

import java.sql.Date;

public class Item {
    public String league;
    public Date date;
    public int itemID;
    public String name;
    public double value;

    public Item(String league, Date date, int itemID, String name, double value) {
        this.league = league;
        this.date = date;
        this.itemID = itemID;
        this.name = name;
        this.value = value;
    }
}
