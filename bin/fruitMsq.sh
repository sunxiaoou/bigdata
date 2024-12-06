#! /bin/sh

put() {
mysql -h$host -u$user -p$passwd $db << EOF 
    INSERT INTO fruit VALUES (101,'🍉',800),(102,'🍓',150),(103,'🍎',120),(104,'🍋',200),(105,'🍊',115),(106,'🍌',110)
EOF
}

add() {
mysql -h$host -u$user -p$passwd $db << EOF 
    INSERT INTO fruit VALUES (107,'🍐',115);
EOF
}

delete() {
mysql -h$host -u$user -p$passwd $db << EOF 
    DELETE FROM fruit WHERE fruit_id = 107
EOF
}

count() {
mysql -h$host -u$user -p$passwd $db << EOF 
    SELECT COUNT(*) FROM $1;
EOF
}

scan() {
mysql -h$host -u$user -p$passwd $db << EOF 
    SELECT * FROM $1
EOF
}

truncate()
{
mysql -h$host -u$user -p$passwd $db << EOF 
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

user=manga
passwd=manga
db=manga

if [ "$#" -gt 2 ]; then
    run "$1" "$2" "$3"
elif [ "$#" -gt 1 ]; then
    run "$1" "$2"
elif [ "$#" -gt 0 ]; then
    run "$1"
else
    echo "Usage: $(basename $0) put|add|delete|count|scan|truncate host table"
fi

