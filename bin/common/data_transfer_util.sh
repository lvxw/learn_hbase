#!/bin/bash
###############################################################################
#Script:        data_transfer_util.sh
#Author:        吕学文<lvxw@fun.tv>
#Date:          2018-11-08
#Description:   数据迁移的相关操作
#Usage:         工具shell，供其他shell脚本引入
#Jira:
###############################################################################

#将hdfs结构化数据插入mysql
function hdfs_to_mysql(){
    table=$1
    columns=$2
    hdfs_dir=$3
    delimiter=$4

    ${SQOOP_INSTALL}/bin/sqoop export  \
     --connect \"${JDBC_CONNECT}\" \
     --username ${ARTEMIS_USER} \
     --password ${ARTEMIS_PWD} \
     --table ${table} \
     --m 1 \
     --export-dir ${hdfs_dir} \
     --input-fields-terminated-by \"${delimiter}\" \
     --columns=\"${columns}\"
}

#加载hdfs天数据到hive外部分区表
load_hdfs_day_data_to_hive(){
    hql_file_path=$1
    hive_db=$2
    hive_table=$3
    hdfs_log_base=$4
    echo "use ${hive_db};" > ${hql_file_path}
    echo "alter table ${hive_table} add if not exists partition(year=\"${year}\", month=\"${month}\", day=\"${day}\") LOCATION \"${hdfs_log_base}\";" >> ${hql_file_path}

    $HIVE_INSTALL/bin/hive -f ${hql_file_path}
}

#加载hdfs小时数据到hive外部分区表
load_hdfs_hour_data_to_hive(){
    hql_file_path=$1
    hive_db=$2
    hive_table=$3
    hdfs_log_base=$4
    echo "use ${hive_db};" > ${hql_file_path}
    for hour in $(seq -w 0 23)
    do
            echo "alter table ${hive_table} add if not exists partition(year=\"${year}\", month=\"${month}\", day=\"${day}\",hour=\"${hour}\") LOCATION \"${hdfs_log_base}/${hour}\";" >> ${hql_file_path}
    done

    $HIVE_INSTALL/bin/hive -f ${hql_file_path}
}

#把mysql数据加载到hive
#接收两个参数：第一个参数: mysql jdbc链接串
#            第二个参数: mysql表名字
#            第三个参数: mysql登录用户名
#            第四个参数: mysql登录密码
#            第五个参数: hive数据库
#            第六个参数: hive表名
function load_mysql_data_to_hive(){
    mysql_jdbc=$1
    mysql_table=$2
    mysql_username=$3
    mysql_password=$4
    hive_db=$5
    hive_table=$6

    ${HIVE_INSTALL}/bin/hive -e "DROP TABLE IF EXISTS ${hive_db}.${hive_table};"
    wait

    ${SQOOP_INSTALL}/bin/sqoop import --connect ${mysql_jdbc} \
    --username ${mysql_username} \
    --password ${mysql_password} \
    --table ${mysql_table} \
    --delete-target-dir \
    --hive-database ${hive_db} \
    --hive-table ${hive_table} \
    --create-hive-table \
    --hive-import \
    --fields-terminated-by "\t" -m 4
    wait
}