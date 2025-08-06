#! /bin/sh

put() {
psql -h$host $db -U$user << EOF
    INSERT INTO fruit VALUES (101,'🍉',800),(102,'🍓',150),(103,'🍎',120),(104,'🍋',200),(105,'🍊',115),(106,'🍌',110)
EOF
}

add() {
psql -h$host $db -U$user << EOF
    INSERT INTO fruit VALUES (107,'🍐',115);
EOF
}

update() {
psql -h$host $db -U$user << EOF
    UPDATE fruit SET price = price + 1 WHERE fruit_id = 107
EOF
}

delete() {
psql -h$host $db -U$user << EOF
    DELETE FROM fruit WHERE fruit_id = 107
EOF
}

count() {
psql -h$host $db -U$user << EOF
    SELECT COUNT(*) FROM $1;
EOF
}

scan() {
psql -h$host $db -U$user << EOF
    SELECT * FROM $1
EOF
}

truncate()
{
psql -h$host $db -U$user << EOF
    TRUNCATE TABLE $1;
EOF
}

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
            put
        else
            echo "Can only put to \"fruit\""
        fi
        ;;
    "add")
        if [ "fruit" = $table ]; then
            add
        else
            echo "Can only add to \"fruit\""
        fi
        ;;
    "update")
        if [ "fruit" = $table ]; then
            update
        else
            echo "Can only update to \"fruit\""
        fi
        ;;
    "delete")
        if [ "fruit" = $table ]; then
            delete
        else
            echo "Can only delete from \"fruit\""
        fi
        ;;
    "count")
        count $table
        ;;
    "scan")
        scan $table
        ;;
    "truncate")
        truncate $table
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
