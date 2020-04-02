package zipkin.storage.mysql;

import org.jooq.Field;
import org.jooq.impl.DSL;

public class UpsertDSL {
    public static <T> Field<T> values(Field<T> field) {
        return DSL.field("VALUES({0})", field.getDataType(), field);
    }
}
