#! /bin/sh

drop_tables()
{
mysql -h$host -u$user -p$passwd $db <<-!end
  drop table if exists export, form, form_detail, fruit
!end
}

## main ##

host=`hostname`
user=manga
passwd=manga
db=manga

drop_tables
