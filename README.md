insert-and-update-
==================


import glob
import multiprocessing
import time 
import MySQLdb
import ast
from datetime import datetime
import sys
num_fetch_threadsm = 5
enclosure_queuem = multiprocessing.JoinableQueue()



def my_strip(x):
    try:
        x = str(x).strip()
        x = MySQLdb.escape_string(x).strip()
    except:
        x = str(x.encode("ascii", "ignore")).strip()
        x = MySQLdb.escape_string(x).strip()

    return x

        


def main(directory, filename):
    db = MySQLdb.connect("localhost","root","6Tresxcvbhy")
    cursor = db.cursor()

    sql = """ create database IF NOT EXISTS  zivame """
    cursor.execute(sql)

    sql =""" use zivame """
    cursor.execute(sql)
    


    f = open(filename)
    for line in f:
        try:
         #   print line
            line_split = ast.literal_eval(str(line).strip())
            line_split[4] = "".join(line_split[4].split(".")[-1].replace(",", "").split())
            line_split[12] = "".join(line_split[12].split(".")[-1].replace(",", "").split())
          
            
            line_split = tuple(map(my_strip, line_split))  
           
            task_id = None

            sql_select = """ select task_id from `zivame_data` where `product_id` = "%s" """ %(str(line_split[0]).strip())
            cursor.execute(sql_select)
            results = cursor.fetchone()
         
          
            if results:   
                task_id = int(results[0])

            if task_id:
                sqlentry = """update zivame_data set selliing_price = "%s", mrp = "%s", color= "%s", size= "%s", dte= "%s", status= "%s"                                            where task_id = "%s" """
                sqlentry = sqlentry %(line_split[4], line_split[12] , line_split[13],  line_split[19], line_split[22], line_split[23], task_id                           )
                result = "updated....................................................................................................."
        
            else:
               # print "hello"
                sqlentry = """insert into zivame_data (product_id, product_title,product_title_zovon ,target_link, selliing_price, category,                                  sub_category,ss_category , ss_link, brand, image_link, sort_image_link, mrp, color, target, product_url, seller,                                    meta_title, meta_desc, size, product_desc, product_spec, dte, status ) values ("%s","%s","%s","%s", "%s", "%s",                                 "%s", "%s","%s", "%s", "%s", "%s", "%s", "%s","%s","%s", "%s",  "%s","%s", "%s", "%s", "%s", "%s", "%s")"""

        

                sqlentry = sqlentry %(line_split) 
                     
               # sqlentry1 = """update zivame_data set product_title_zovon = "%s", ss_link = "%s", sort_image_link = "%s",                                                      where task_id = "%s" """
                #sqlentry1 = sqlentry1 %(product_title_zovon1,ss_link1,sort_image_link1, task_id )

                result = "inserted..................................................................................................."
        
           
        
            try:
                cursor.execute(sqlentry)
                #cursor.execute(sqlentry1)
                db.commit()
                print result

            except:
                try:
                    db.ping()
                    cursor.execute(sqlentry)
                    cursor.execute(sqlentry1)
                    db.commit()
                    print "connection reconnect..................................................................................."
                    print result
                except:
                    db.rollback()
                    print "rollback. .............................................................................................." 
            
        except:
            pass    
        
    db.close()



def main3(i, q):
    for directory,  filename in iter(q.get, None):
        try:
          #  print filename
            main(directory, filename)
        except:
            pass

        time.sleep(2)
        q.task_done()

    q.task_done()



def supermain():
    directory = "/home/desktop/anit/zivame/zivame_dir"
    flptrn = "%s/*.csv" %(directory)
    csv_file_list = glob.glob(flptrn)
    #print csv_file_list
    
    procs = []

    for i in range(num_fetch_threadsm):
        procs.append(multiprocessing.Process(name = str(i), target=main3, args=(i, enclosure_queuem,)))
        procs[-1].start()

    for filename in csv_file_list:
        enclosure_queuem.put((directory, filename.strip()))

    enclosure_queuem.join()

    for p in procs:
        enclosure_queuem.put(None)

    enclosure_queuem.join()

    for p in procs:
        p.join(1800)    



if __name__=="__main__":
    tstart = datetime.now()
   # directory = "/home/desktop/working_sites/flipkart_site/flipkart/flipkart_dir190614"
    directory = "zivame_dir"
    supermain()
    #filename ="zivame_dir/Accessories New.csv"
    #main(directory, filename)
    tend = datetime.now()
    print tend - tstart

