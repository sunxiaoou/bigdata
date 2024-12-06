package xo.sap.jco;

import java.util.Objects;

/**
 * Represents the metadata of a field in the ODP parser.
 */
public class FieldMeta {
    private boolean keyFlag;
    private int outputLength;
    private String description;
    private boolean transfer;
    private boolean selEqual;
    private int length;
    private boolean selBetween;
    private boolean selNonDefault;
    private int decimals;
    private String type;
    private boolean selPattern;
    private String name;
    private boolean deltaField;

    // Constructor
    public FieldMeta(boolean keyFlag, int outputLength, String description, boolean transfer, boolean selEqual,
                     int length, boolean selBetween, boolean selNonDefault, int decimals, String type,
                     boolean selPattern, String name, boolean deltaField) {
        this.keyFlag = keyFlag;
        this.outputLength = outputLength;
        this.description = description;
        this.transfer = transfer;
        this.selEqual = selEqual;
        this.length = length;
        this.selBetween = selBetween;
        this.selNonDefault = selNonDefault;
        this.decimals = decimals;
        this.type = type;
        this.selPattern = selPattern;
        this.name = name;
        this.deltaField = deltaField;
    }

    // Getters and Setters
    public boolean getKeyFlag() {
        return keyFlag;
    }

    public void setKeyFlag(boolean keyFlag) {
        this.keyFlag = keyFlag;
    }

    public int getOutputLength() {
        return outputLength;
    }

    public void setOutputLength(int outputLength) {
        this.outputLength = outputLength;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean getTransfer() {
        return transfer;
    }

    public void setTransfer(boolean transfer) {
        this.transfer = transfer;
    }

    public boolean getSelEqual() {
        return selEqual;
    }

    public void setSelEqual(boolean selEqual) {
        this.selEqual = selEqual;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public boolean getSelBetween() {
        return selBetween;
    }

    public void setSelBetween(boolean selBetween) {
        this.selBetween = selBetween;
    }

    public boolean getSelNonDefault() {
        return selNonDefault;
    }

    public void setSelNonDefault(boolean selNonDefault) {
        this.selNonDefault = selNonDefault;
    }

    public int getDecimals() {
        return decimals;
    }

    public void setDecimals(int decimals) {
        this.decimals = decimals;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean getSelPattern() {
        return selPattern;
    }

    public void setSelPattern(boolean selPattern) {
        this.selPattern = selPattern;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean getDeltaField() {
        return deltaField;
    }

    public void setDeltaField(boolean deltaField) {
        this.deltaField = deltaField;
    }

    @Override
    public String toString() {
        return "FieldMeta{" +
                "keyFlag='" + keyFlag + '\'' +
                ", outputLength=" + outputLength +
                ", description='" + description + '\'' +
                ", transfer='" + transfer + '\'' +
                ", selEqual='" + selEqual + '\'' +
                ", length=" + length +
                ", selBetween='" + selBetween + '\'' +
                ", selNonDefault='" + selNonDefault + '\'' +
                ", decimals=" + decimals +
                ", type='" + type + '\'' +
                ", selPattern='" + selPattern + '\'' +
                ", name='" + name + '\'' +
                ", deltaField='" + deltaField + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMeta fieldMeta = (FieldMeta) o;
        return outputLength == fieldMeta.outputLength &&
                length == fieldMeta.length &&
                decimals == fieldMeta.decimals &&
                Objects.equals(keyFlag, fieldMeta.keyFlag) &&
                Objects.equals(description, fieldMeta.description) &&
                Objects.equals(transfer, fieldMeta.transfer) &&
                Objects.equals(selEqual, fieldMeta.selEqual) &&
                Objects.equals(selBetween, fieldMeta.selBetween) &&
                Objects.equals(selNonDefault, fieldMeta.selNonDefault) &&
                Objects.equals(type, fieldMeta.type) &&
                Objects.equals(selPattern, fieldMeta.selPattern) &&
                Objects.equals(name, fieldMeta.name) &&
                Objects.equals(deltaField, fieldMeta.deltaField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyFlag, outputLength, description, transfer, selEqual, length, selBetween, selNonDefault,
                decimals, type, selPattern, name, deltaField);
    }
}
