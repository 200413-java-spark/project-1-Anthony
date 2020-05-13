package items;

import java.sql.Date;

public class UniqueArmor extends Item{
    public String type;
    public String baseType;
    public int links;

    public UniqueArmor(String league, Date date, int itemID, String type, String name, String baseType, int links, double value, String confidence) {
        super(league, date, itemID, name, value, confidence);
        this.type = "UniqueArmor";
        this.baseType = baseType;
        this.links = links;
    }
}
