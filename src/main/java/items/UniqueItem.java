package items;

import java.sql.Date;

public class UniqueItem extends Item {
    public String type;
    public String baseType;
    public int links;

    public UniqueItem(String league, Date date, int itemID, String type, String name, String baseType, double value) {
        super(league, date, itemID, name, value);
        this.type = type;
        this.baseType = baseType;
    }
}
