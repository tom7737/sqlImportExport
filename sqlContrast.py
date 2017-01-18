#coding=utf-8
'''
Created on 2017-1-11
用于对比创天下1.0数据库与创天下2.0数据库中栏目表的变化
@author: admin
'''
import mysql.connector

conn_old = mysql.connector.connect(user="root",password="",database='entrepreneurs',use_unicode=True)
conn_new = mysql.connector.connect(user="root",password="",database='jeecmsV8',use_unicode=True)
cursor_old = conn_old.cursor()
cursor_new = conn_new.cursor()

tables_old = []
tables_new = []
cursor_old.execute("select jc_channel.channel_id,jc_channel.parent_id,jc_channel_ext.channel_name from jc_channel inner join jc_channel_ext on jc_channel.channel_id = jc_channel_ext.channel_id")
tables_old = cursor_old.fetchall()
cursor_new.execute("select jc_channel.channel_id,jc_channel.parent_id,jc_channel_ext.channel_name from jc_channel inner join jc_channel_ext on jc_channel.channel_id = jc_channel_ext.channel_id")
tables_new = cursor_new.fetchall()

def getOldById(cid):
    for x in tables_old:
        if x[0]==cid:
            return x
    return None

def getNewById(cid):
    for x in tables_new:
        if x[0]==cid:
            return x
    return None

def getOldFullName(cid):
    for x in tables_old:
        if x[0]==cid:
            if x[1] != None :
                return getOldFullName(x[1])+"--"+x[2]
            else:
                return x[2]

def getNewFullName(cid):
    for x in tables_new:
        if x[0]==cid:
            if x[1] != None :
                return getNewFullName(x[1])+"--"+x[2]
            else:
                return x[2]  
for x in tables_old:
    old_cid = x[0]
    old_parentId = x[1]
    old_channel_name = x[2]
    #print old_channel_name.encode('utf-8')
    new_channel = getNewById(old_cid)
    if x[1]!=new_channel[1] or x[2]!=new_channel[2]:
        print "%d %s > %s " % (old_cid,getOldFullName(old_cid),getNewFullName(new_channel[0]))
    
    
cursor_old.close()
conn_old.close()
cursor_new.close()
conn_new.close()


