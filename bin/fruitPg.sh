#! /bin/sh

run() {
    op=$1
    host=$2
    table=$3

    if [ -z "$host" ]; then
        host=`hostname`
    fi

    if [ -z "$table" ]; then
        table="fruit"
    fi

    case $op in
    "put")
        if [ "fruit" = $table ]; then
            data="(101,'🍉',800),(102,'🍓',150),(103,'🍎',120),(104,'🍋',200),(105,'🍊',115),(106,'🍌',110)"
        elif [ "types" = $table ]; then
            data="(1, 1234567890123, 3.14159, 12345.67, '2025-07-31 12:34:56'),(2, 9876543210987, 2.71828, 76543.21, '2024-12-31 23:59:59')"
        else
            echo "Can only put to \"fruit\" or \"types\""
            exit 1
        fi
psql -h$host $db -U$user << EOF
    INSERT INTO $table VALUES $data;
EOF
        ;;
    "add")
        if [ "fruit" = $table ]; then
            data="(107,'🍐',115)"
        elif [ "types" = $table ]; then
            data="(-1, -9876543210987, -2.71828, -98765.43, '2025-07-31 23:45:01')"
        else
            echo "Can only add to \"fruit\" or \"types\""
            exit 1
        fi
psql -h$host $db -U$user << EOF
    INSERT INTO $table VALUES $data;
EOF
        ;;
    "update")
        if [ "fruit" = $table ]; then
            dml="UPDATE $table SET price = price + 1 WHERE fruit_id = 107"
        elif [ "types" = $table ]; then
            dml="UPDATE $table SET float_val = float_val + 0.00001 WHERE id = -1"
        else
            echo "Can only update to \"fruit\" or \"types\""
            exit 1
        fi
psql -h$host $db -U$user << EOF
    $dml
EOF
        ;;
    "delete")
        if [ "fruit" = $table ]; then
            dml="DELETE FROM $table WHERE fruit_id = 107"
        elif [ "types" = $table ]; then
            dml="DELETE FROM $table WHERE id = -1"
        else
            echo "Can only delete from \"fruit\" or \"types\""
        fi
psql -h$host $db -U$user << EOF
    $dml
EOF
        ;;
    "count")
psql -h$host $db -U$user << EOF
    SELECT COUNT(*) FROM $table;
EOF
        ;;
    "scan")
psql -h$host $db -U$user << EOF
    SELECT * FROM $table;
EOF
        ;;
    "truncate")
psql -h$host $db -U$user << EOF
    TRUNCATE TABLE $table;
EOF
        ;;
    *)
        echo "Unknown op: $op"    
        exit 1
        ;;    
    esac
}


## main ##

db=postgres
user=manga

if [ "$#" -gt 2 ]; then
    run "$1" "$2" "$3"
elif [ "$#" -gt 1 ]; then
    run "$1" "$2"
elif [ "$#" -gt 0 ]; then
    run "$1"
else
    echo "Usage: $(basename $0) put|add|update|delete|count|scan|truncate host table"
fi
