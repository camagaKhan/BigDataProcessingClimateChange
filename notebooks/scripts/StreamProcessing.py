try :
    import findspark as ff
    ff.init('/home/camagakhan/spark') # this is my spark directory
    import pyspark

    print('pyspark found')
except: 
    print('[ERROR] Pyspark not found')
