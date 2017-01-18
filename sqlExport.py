#coding=utf-8
'''
Created on 2017-1-11
用于将创天下1.0数据库导入创天下2.0数据库。
@author: admin
'''
import mysql.connector
from mysql.connector.errors import IntegrityError

conn_old = mysql.connector.connect(user="root",password="",database='entrepreneurs',use_unicode=True)
conn_new = mysql.connector.connect(user="root",password="",database='jeecmsV8',use_unicode=True)
cursor_old = conn_old.cursor()
cursor_new = conn_new.cursor()



tables_old = []
tables_new = []
cursor_old.execute("show tables")
tables_old = cursor_old.fetchall()
cursor_new.execute("set FOREIGN_KEY_CHECKS=0 ")
cursor_new.execute("show tables")
tables_new = cursor_new.fetchall()
#1、老表有，新表有：替换
#2、老表有、新表没有：新建表，复制数据
#3、老表没有、新表有：新表数据不动
for x in tables_old:
    if x in tables_new:
        #if x.__len__() > -1 :
        #    continue
        #老表的结构
        cursor_old.execute('desc '+x[0])
        desc_table_old = cursor_old.fetchall()
        desc_table_old1 = []
        for item1 in desc_table_old:
            desc_table_old1.append(item1[0])
        #print desc_table_old1
        #新表的结构
        cursor_new.execute('desc '+x[0])
        desc_table_new = cursor_new.fetchall()
        desc_table_new1 = []
        for item2 in desc_table_new:
            desc_table_new1.append(item2[0])
        #print desc_table_new1
        #print desc_table_old
        #清空新表
        cursor_new.execute("delete from "+x[0])
        cursor_new.rowcount
        #取新表和老表的表结构交集
        table_structure  = [val for val in desc_table_old1 if val in desc_table_new1]
        #print x[0],table_structure
        str1 = "";
        for items11 in table_structure:
            str1+=items11+",";
        cursor_old.execute("select count(*) from "+x[0])
        count = cursor_old.fetchall()
        tempcount = 0
        
        while tempcount<count[0][0]:
            #获取老表的数据
            sql = "select "+str1[0:str1.__len__()-1]+" from "+x[0]+" limit %d,%d" % (tempcount,tempcount+1000)
            cursor_old.execute(sql)
            table_date_old = cursor_old.fetchall()
            str2 = "("
            for i in range(0,table_structure.__len__()):
                str2 +="%s,"
            str2 = str2[0:str2.__len__()-1]+")"
            for items21 in table_date_old:
                #替换新表中的数据
                sql2 = "replace into %s(%s) values %s" % (x[0],str1[0:str1.__len__()-1],str2)
                try:
                    cursor_new.execute(sql2,items21)
                except IntegrityError,e:
                    print x[0],items21
                    print e
            tempcount = tempcount+1000
    else:
        print x[0]," not in tables_new"
    
    conn_new.commit()



# TODO 打印新库比老库多出来的表
for y in tables_new:
    if y not in tables_old:
        print y[0],"not in tables_old"


cursor_old.close()
conn_old.close()
cursor_new.close()
conn_new.close()


