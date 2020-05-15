package items;

import java.io.Serializable;

public class UniqueItem extends Item {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public String type;
    public String baseType;
    public int links;

    public UniqueItem(String league, String date, int itemID, String type, String name, String baseType, double value) {
        super(league, date, itemID, name, value);
        this.type = type;
        this.baseType = baseType;
    }

    public String getLeague() {
        return this.league;
    }

    public String getDate() {
        return this.date;
    }

    public int getItemID() {
        return this.itemID;
    }

    public String getType() {
        return this.type;
    }

    public String getName() {
        return this.name;
    }

    public String getBaseType() {
        return this.baseType;
    }

    public double getValue() {
        return this.value;
    }
}
