# 尝试连接oracle
import cx_Oracle


db=cx_Oracle.connect('scott/cf6024584@localhost/orcl')
cr=db.cursor()
sql="select 1 from dual"
cr.execute(sql)
rs=cr.fetchall()
print ("the query result is "%rs)
print('hello world!')# 随便
cr.close()
db.close()