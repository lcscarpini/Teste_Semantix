# Teste Semantix
# Datasets:
# 	Julho -> ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
# 	Agosto -> ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz


from pyspark import SparkConf, SparkContext
from operator import add


conf = (SparkConf()
         .setMaster("local")
         .setAppName("SPark"))
sc = SparkContext(conf = conf)


Julho = sc.textFile('NASA_access_log_Jul95')
Julho = Julho.cache()

Agosto = sc.textFile('NASA_access_log_Aug95')
Agosto = Agosto.cache()


# Conta os Hosts Distintos
Julho_count = Julho.flatMap(lambda line: line.split(' ')[0]).distinct().count()
Agosto_count = Agosto.flatMap(lambda line: line.split(' ')[0]).distinct().count()
print('Hosts em Julho: %s' % Julho_count)
print('Hosts em Agosto %s' % Agosto_count)


# Conta os erros 404
def Code_404(line):
    try:
        code = line.split(' ')[-2]
        if code == '404':
            return True
    except:
        pass
    return False
    
Julho_404 = Julho.filter(Code_404).cache()
Agosto_404 = Agosto.filter(lambda line: line.split(' ')[-2] == '404').cache()

print('Numero de erro 404 em Julho: %s' % Julho_404.count())
print('Numero de erro 404 em Agosto: %s' % Agosto_404.count())

# Conta o total de byte
def byteCountT(rdd):
    def byte_count(line):
        try:
            count = int(line.split(" ")[-1])
            if count < 0:
                raise ValueError()
            return count
        except:
            return 0
        
    count = rdd.map(byte_count).reduce(add)
    return count

print('Total byte em Julho: %s' % byteCountT(Julho))
print('Total byte em Agosto: %s' % byteCountT(Agosto))


sc.stop()

