package items;

import java.io.Serializable;
import java.util.Date;

public class Item implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public String league;
    public String date;
    public int itemID;
    public String name;
    public double value;

    public Item(String league, String date, int itemID, String name, double value) {
        this.league = league;
        this.date = date;
        this.itemID = itemID;
        this.name = name;
        this.value = value;
    }
}
