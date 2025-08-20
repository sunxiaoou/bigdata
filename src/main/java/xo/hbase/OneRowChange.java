package xo.hbase;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Holds metadata and data for a row change set, which is a change of a unique
 * type (INSERT/UPDATE/DELETE) involving one or more rows in a single table. Row
 * changes include "keys," which are effectively the before images of rows that
 * can be used to identify rows to update or delete, and "values," which are the
 * after images of rows that should be inserted or updated.
 */
public class OneRowChange implements Serializable , Cloneable
{
    public enum ActionType
    {
        INSERT, DELETE, UPDATE, INSERTONDUP
    }

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(OneRowChange.class);

    /*
     * following types make it possible to apply changes by prepared statements.
     * One RowChangeData corresponds with one prepared statement. Binding to
     * variables are made from keys and columnvals lists.
     */
    /* ColumnSpec is a "header" for columnVal arrays */
    public class ColumnSpec implements Serializable , Cloneable
    {
        private static final long serialVersionUID = 1L;
        private int               index;     // Start with 1
        private String            name;
        private int               type;                 // Type assignment from
                                                         // java.sql.Types
        private boolean           signed;
        private int               length;
        private boolean           notNull;              // Is the column a NOT
                                                         // NULL column
        private boolean           blob;
        private String            typeDescription;
        private boolean           isVirtual = false;
        private boolean isFuncValue = false;
        private String  funcName;
        private boolean           isDistributeCol;
      


        public boolean isBlob()
        {
            return blob;
        }
        public void setBlob(boolean blob)
        {
            this.blob = blob;
        }
        public ColumnSpec()
        {
            this.name = null;
            this.type = java.sql.Types.NULL;
            this.length = 0;
            this.notNull = false;
            this.signed = true;
            this.blob = false;
        }

        public ColumnSpec(String name, int index, int type, int length, String typeDescription, boolean notNull)
        {
            this.index = index;
            this.name = name;
            this.type = type;
            this.length = length;
            this.notNull = notNull;
            this.signed = true;
            this.blob = false;
            this.typeDescription = typeDescription;
        }

        {
            this.name = null;
            this.type = java.sql.Types.NULL;
            this.length = 0;
            this.notNull = false;
            this.signed = true;
            this.blob = false;
        }

        public ColumnSpec(ColumnSpec spec)
        {
            this();
            this.name = spec.getName();
            this.type = spec.getType();
            this.length = spec.getLength();
            this.notNull = spec.isNotNull();
            this.signed = !spec.isUnsigned();
            this.blob = spec.isBlob();
            this.typeDescription = spec.getTypeDescription();
        }
        public int getIndex()
        {
            return this.index;
        }
        public void setName(String name)
        {
            this.name = name;
        }
        public void setType(int type)
        {
            this.type = type;
        }
        public int getLength()
        {
            return length;
        }
        public void setLength(int length)
        {
            this.length = length;
        }
        public boolean isNotNull()
        {
            return notNull;
        }
        public void setNotNull(boolean notNull)
        {
            this.notNull = notNull;
        }
        public String getName()
        {
            return name;
        }
        public int getType()
        {
            return type;
        }
        public void setIndex(int index)
        {
            this.index = index;
        }
        public void setSigned(boolean signed)
        {
            this.signed = signed;
        }
        public boolean isUnsigned()
        {
            return !signed;
        }
        public String getTypeDescription()
        {
            return typeDescription;
        }
        public void setTypeDescription(String typeDescription)
        {
            this.typeDescription = typeDescription;
        }

        public boolean isVirtual() {
            return isVirtual;
        }

        public void setVirtual(boolean virtual) {
            isVirtual = virtual;
        }

        public boolean isFuncValue() {
            return isFuncValue;
        }

        public void setFuncValue(boolean funcValue) {
            isFuncValue = funcValue;
        }

        public String getFuncName() {
            return funcName;
        }

        public void setFuncName(String funcName) {
            this.funcName = funcName;
        }

        public boolean isDistributeCol() {
            return isDistributeCol;
        }

        public void setDistributeCol(boolean distributeCol) {
            isDistributeCol = distributeCol;
        }

        /**
         * {@inheritDoc}
         * 
         * @see Object#toString()
         */
        public String toString()
        {
            StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
            sb.append(" name=").append(name);
            sb.append(" index=").append(index);
            sb.append(" type=").append(type);
            sb.append(" length=").append(length);
            sb.append(" description=").append(typeDescription);
            sb.append(" isVirtual=").append(isVirtual);
            return sb.toString();
        }
        public ColumnSpec clone() {
            ColumnSpec  repl = null;  
          try{  
              repl = (ColumnSpec)super.clone(); 
          }catch(CloneNotSupportedException e) {  
              logger.warn(ExceptionUtils.getStackTrace(e));
          }  
          return repl;  
        }   
    }
    public class ColumnVal implements Serializable ,Cloneable
    {
        private static final long serialVersionUID = 1L;
        private Serializable      value;
        public void setValueNull()
        {
            value = null;
        }
        public void setValue(Serializable value)
        {
            this.value = value;
        }
        public Object getValue()
        {
            return value;
        }
        public String toString()
        {
            StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
            sb.append(" value=").append(value);
            return sb.toString();
        }
        @Override  
        public ColumnVal clone() {
            ColumnVal  repl = null;  
          try{  
              repl = (ColumnVal)super.clone(); 
          }catch(CloneNotSupportedException e) {  
              logger.warn(ExceptionUtils.getStackTrace(e));
          }  
          return repl;  
        }   
    }
    
    private String                          schemaName;
    private String                          tableName;
    private String                          priName;
    private String                          onlyPrimaryKeyNames;
    private ActionType                      action;
    /* column specifications for key and data columns */
    private ArrayList<ColumnSpec>           keySpec;
    private ArrayList<ColumnSpec>           columnSpec;
    /* values for key components (may be empty) */
    private ArrayList<ArrayList<ColumnVal>> keyValues;
    /* values for data column components */
    private ArrayList<ArrayList<ColumnVal>> columnValues;
    private long                            tableId;
    // Type cache to enable filters to check whether particular types are
    // present. This value is not serialized.
    private HashMap<Integer, Integer>       typeCountCache;
    // Row number range in the whole transaction
    private int                             startRowNo;
    private int                             endRowNo;
    private boolean                         isAuditTable4DMLTrack = false;
    private String                          baseTableName;

    public void setStartRowNo(int rn) { this.startRowNo = rn; }
    public void setEndRowNo(int rn) { this.endRowNo = rn; }
    public int  getStartRowNo() { return this.startRowNo; }
    public int  getEndRowNo() { return this.endRowNo; }
    
    public boolean isAuditTable4DMLTrack() {
        return isAuditTable4DMLTrack;
    }

    public void setAuditTable4DMLTrack(boolean auditTable4DMLTrack) {
        isAuditTable4DMLTrack = auditTable4DMLTrack;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public void setBaseTableName(String baseTableName) {
        this.baseTableName = baseTableName;
    }

    public void setPrimaryKeyName(String pri) {
        this.priName= pri;
    }
    
    public String getPrimaryKeyName() {
        return this.priName;
    }

    public String getOnlyPrimaryKeyNames() {
        return onlyPrimaryKeyNames;
    }

    public void setOnlyPrimaryKeyNames(String onlyPrimaryKeyNames) {
        this.onlyPrimaryKeyNames = onlyPrimaryKeyNames;
    }

    public ArrayList<ColumnSpec> getColumnSpec()
    {
        return columnSpec;
    }
    public void setColumnSpec(ArrayList<ColumnSpec> columnSpec)
    {
        // Set the key specifications and invalidate type cache.
        this.columnSpec = columnSpec;
        this.typeCountCache = null;
    }
    public ArrayList<ArrayList<ColumnVal>> getColumnValues()
    {
        return columnValues;
    }
    public void setColumnValues(ArrayList<ArrayList<ColumnVal>> columnValues)
    {
        this.columnValues = columnValues;
    }
    public ArrayList<ColumnSpec> getKeySpec()
    {
        return keySpec;
    }
    public void setKeySpec(ArrayList<ColumnSpec> keySpec)
    {
        // Set the key specifications and invalidate type cache.
        this.keySpec = keySpec;
        this.typeCountCache = null;
    }
    public ArrayList<ArrayList<ColumnVal>> getKeyValues()
    {
        return keyValues;
    }
    public void setKeyValues(ArrayList<ArrayList<ColumnVal>> keyValues)
    {
        this.keyValues = keyValues;
    }
    public ActionType getAction()
    {
        return action;
    }
    public void setAction(ActionType action)
    {
        this.action = action;
    }
    public String getSchemaName()
    {
        return schemaName;
    }
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }
    public String getTableName()
    {
        return tableName;
    }
    public void setTableName(String tableName)
    {  
        this.tableName = tableName;
    }
    public OneRowChange(String schemaName, String tableName, ActionType action)
    {
        this();
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.action = action;
    }
    public OneRowChange()
    {
        keySpec = new ArrayList<ColumnSpec>();
        keyValues = new ArrayList<ArrayList<ColumnVal>>();
        columnSpec = new ArrayList<ColumnSpec>();
        columnValues = new ArrayList<ArrayList<ColumnVal>>();
        this.tableId = -1;
    }
    public void setTableId(long tableId)
    {
        this.tableId = tableId;
    }
    public long getTableId()
    {
        return tableId;
    }
    /**
     * Returns the count of a particular column specification type within either
     * the values or keys. If the count is 0, the type is not present.
     */
    public int typeCount(int aType)
    {
        if (this.typeCountCache == null)
        {
            HashMap<Integer, Integer> countCache = new HashMap<Integer, Integer>();
            // Check values first and only if the specifications array exists.
            if (this.columnSpec != null)
            {
                for (ColumnSpec cs : columnSpec)
                {
                    int csType = cs.getType();
                    Integer count = countCache.get(csType);
                    if (count == null)
                        countCache.put(csType, 1);
                    else
                        countCache.put(csType, count + 1);
                }
            }
            // Check keys next and only if the specifications array exists.
            if (this.keySpec != null)
            {
                for (ColumnSpec ks : keySpec)
                {
                    int ksType = ks.getType();
                    Integer count = countCache.get(ksType);
                    if (count == null)
                        countCache.put(ksType, 1);
                    else
                        countCache.put(ksType, count + 1);
                }
            }
            // Store the completed hash map as the cache.
            typeCountCache = countCache;
        }
        // Look up the count for this type.
        Integer count = typeCountCache.get(aType);
        if (count == null)
            return 0;
        else
            return count;
    }
    /**
     * Returns true if the change set includes the type argument.
     */
    public boolean hasType(int aType)
    {
        return (this.typeCount(aType) > 0);
    }
    
    @Override  
    public Object clone() {  
        OneRowChange repl = null;
      try{  
          repl = (OneRowChange)super.clone();
      }catch(CloneNotSupportedException e) {  
          logger.warn(ExceptionUtils.getStackTrace(e));
      }  
      repl.keySpec = new  ArrayList<ColumnSpec>();
      repl.keyValues= new ArrayList<ArrayList<ColumnVal>>();
      
      for (int i= 0;i<keySpec.size();i++) {
          ColumnSpec newSpec = null;
          ColumnSpec spec = (ColumnSpec) keySpec.get(i);
          if (spec != null)
              newSpec = spec.clone();
          repl.keySpec.add(newSpec);
      }
      for (int i= 0;i<keyValues.size();i++) {
          ArrayList<ColumnVal> arrCol= new ArrayList<ColumnVal>();
          ArrayList<ColumnVal> trueArrCol = keyValues.get(i);
          for (int j =0 ;j <trueArrCol.size();j++) {
              ColumnVal newVal = null;
              ColumnVal value= (ColumnVal) trueArrCol.get(j);
              if (value != null)
                  newVal = value.clone();
              arrCol.add(newVal);
          }
          repl.keyValues.add(arrCol);
      }

        repl.columnSpec = new  ArrayList<ColumnSpec>();
        repl.columnValues = new ArrayList<ArrayList<ColumnVal>>();

        for (int i= 0;i<columnSpec.size();i++) {
            ColumnSpec newSpec = null;
            ColumnSpec spec= (ColumnSpec) columnSpec.get(i);
            if (spec != null)
                newSpec = spec.clone();
            repl.columnSpec.add(newSpec);
        }
        for (int i= 0;i<columnValues.size();i++) {
            ArrayList<ColumnVal> arrCol= new ArrayList<ColumnVal>();
            ArrayList<ColumnVal> trueArrCol = columnValues.get(i);
            for (int j =0 ;j <trueArrCol.size();j++) {
                ColumnVal newVal = null;
                ColumnVal value= (ColumnVal) trueArrCol.get(j);
                if (value != null)
                    newVal = value.clone();
                arrCol.add(newVal);
            }
            repl.columnValues.add(arrCol);
        }
        repl.setStartRowNo(this.startRowNo);
        repl.setEndRowNo(this.endRowNo);
        return repl;
    }

    @Override
    public String toString() {
        return "OneRowChange{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", priName='" + priName + '\'' +
                ", onlyPrimaryKeyNames='" + onlyPrimaryKeyNames + '\'' +
                ", action=" + action +
                ", keySpec=" + keySpec +
                ", columnSpec=" + columnSpec +
                ", keyValues=" + keyValues +
                ", columnValues=" + columnValues +
                ", tableId=" + tableId +
                ", startRowNo=" + startRowNo +
                ", endRowNo=" + endRowNo +
                ", isAuditTable4DMLTrack=" + isAuditTable4DMLTrack +
                ", baseTableName='" + baseTableName + '\'' +
                '}';
    }
}
