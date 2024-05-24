package xo.dumper;

import java.util.Objects;

public class ObjIdentifier {
    private String type;
    private String schema;
    private String name;

    public ObjIdentifier(){
    }

    public ObjIdentifier(String type, String schema, String name) {
        this.type = type;
        this.schema = schema;
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjIdentifier)) {
            return false;
        }
        ObjIdentifier that = (ObjIdentifier) o;
        return Objects.equals(type, that.type) &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, schema, name);
    }

    @Override
    public String toString() {
        return "ObjIdentifier{" +
                "type='" + type + '\'' +
                ", schema='" + schema + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
