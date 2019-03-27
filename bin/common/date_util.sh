#!/bin/bash
###############################################################################
#Script:        date_util.sh
#Author:        吕学文<lvxw@fun.tv>
#Date:          2018-11-08
#Description:   常用处理时间的函数
#Usage:         工具shell，供其他shell脚本引入
#Jira:
###############################################################################

#初始化脚本运行参数data_date日期后一天的 天（几号）、星期
function init_util(){
    week_day=`get_week_day $1`
    month_day=`get_month_day $1`
}

#获取传入时间的前后某n天的日期
#接收两个参数：第一个参数
#                      日期字符传，如 2018-02-02 或者 20180202 或者 2018/02/02
#            第二个参数
#                      过去或将来几天，负数表示过去，整数表示将来。例如 -2  1
function get_next_or_before_date(){
    local param_date=$1
    local days=$2
    local delimiter=${param_date:4:1}
    if [ ${delimiter} != '-' -a  ${delimiter} != '/' ]
    then
        delimiter=""
    fi

	local return_date=$(date -d @$((`date -d ${param_date} +%s` + 86400*${days})) +%Y${delimiter}%m${delimiter}%d)
	echo  $return_date
}

#获取传入时间的星期
#接收两个参数：第一个参数
#                      日期字符传，如 2018-02-02 或者 20180202 或者 2018/02/02
function get_week_day(){
    local param_date=$1
    local return_week=$(date -d @$((`date -d "$param_date" +%s`)) +%w)
    echo ${return_week}
}

#获取一个周的开始日期
#接收两个参数：第二个参数
#                       日期字符传，如 2018-02-02 或者 20180202 或者 2018/02/02
#            第二个参数
#                       把自然周中的周几设置为一个周的开始
function get_month_day(){
    local param_date=$1
    local delimiter=${param_date:4:1}
    if [ ${delimiter} != '-' -a  ${delimiter} != '/' ]
    then
        local return_month=${param_date:6:2}
    else
        local return_month=${param_date:8:2}
    fi
    echo ${return_month}
}

function get_start_week_date_by_set_start_week(){
    local param_date=$1
    local start_week=$2

    for i in `seq 0 6`
    do
        if [[ ${i} -ge ${start_week} ]]
        then
            re=$((${i}-${start_week}))
            new_week_arr[${i}]=$((${i}-${start_week}))
        else
            new_week_arr[${i}]=$((7-${start_week}+${i}))
        fi
    done

    local cur_week=`get_week_day ${param_date}`
    local return_date=`get_next_or_before_date ${param_date} -${new_week_arr[${cur_week}]}`
    echo ${return_date}

}

