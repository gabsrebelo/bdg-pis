from pyspark import SparkConf, SparkContext
import argparse

conf = SparkConf().setMaster("local").setAppName("BD")
sc = SparkContext(conf = conf)

argparser = argparse.ArgumentParser()
argparser.add_argument("input", help="input file with products")
args = argparser.parse_args()

def parse(filename, total):
    '''
    Parser que le o .txt e a saida eh um generator dos produtos na estrutura de dicionario
    '''
    IGNORE_FIELDS = ['Total items', 'reviews']
    f = open(filename, 'r')
    lines = f.readlines()
    entry = {}
    categories = []
    reviews = []
    similar_items = []
  
    for line in lines[0:total]:
        colonPos = line.find(':')

        if line.startswith("Id"):
            if reviews:
                entry["reviews"] = reviews
            if categories:
                entry["categories"] = categories
            yield entry
            entry = {}
            categories = []
            reviews = []
            rest = line[colonPos+2:]
            entry["id"] = rest[1:-1]
      
        elif line.startswith("similar"):
            similar_items = line.split()[2:]
            entry['similar_items'] = similar_items

    # "cutomer" is typo of "customer" in original data
        elif line.find("cutomer:") != -1:
            review_info = line.split()
            reviews.append({'customer_id': review_info[2], 
                          'rating': int(review_info[4]), 
                          'votes': int(review_info[6]), 
                          'helpful': int(review_info[8]),
                           'date':review_info[0]})

        elif line.startswith("   |"):
            categories.append(line[0:-1].replace(' ',''))

        elif colonPos != -1:
            eName = line[:colonPos]
            rest = line[colonPos+2:]
            if not eName in IGNORE_FIELDS:
                if eName[0] == ' ':
                    eName = eName[2:]
                entry[eName] = rest[0:-1].replace("'","''")

    if reviews:
        entry["reviews"] = reviews
    if categories:
        entry["categories"] = categories
    
    yield entry

def read_file(file_path):

    line_num = sum(1 for line in open(file_path))
    result = []
    for e in parse(file_path, total=line_num):
        if e:
            result.append(e)
    return result


product_dict_list = read_file(args.input)
products = sc.parallelize(product_dict_list)

def reviews(ASIN):
    '''
    Funcao que dado produto, lista os 5 comentários mais uteis e com maior avaliacao e os 5 comentarios mais uteis e com menor avaliacao
    '''
    #filter pra pegar somente o rdd com o ASIN especifico
    product = products.filter(lambda x: x["ASIN"] == ASIN).first()
    #Agora temos um RDD no qual cada elemento eh um comentario.
    reviews = sc.parallelize(product["reviews"])
    # map para transformar cada dict em uma tupla (x,x,x,x,x)
    reviews_tuple = reviews.map(lambda x: (x["date"],x["customer_id"],x["rating"],x["votes"],x["helpful"]))
    reviews_greater = reviews_tuple.sortBy(lambda x: (x[4],x[2]), False).take(5)
    print("5 comentarios mais uteis e com maior avaliacao:")
    for r in reviews_greater:
        print(r)
    reviews_lesser = reviews_tuple.sortBy(lambda x: (x[4],x[2]), (False,True)).take(5)
    print("5 comentarios mais uteis e com menor avaliacao:")
    for r in reviews_lesser:
        print(r)

def similar(ASIN):
    '''
    Funcao que dado um produto, lista os produtos similares com maiores vendas do que ele
    '''
    #RDD chave-valor (ASIN,salesrank)
    prod_ASIN_salesrank = products.filter(lambda x: "salesrank" in x).map(lambda x: (x["ASIN"],int(x["salesrank"])))
    product = products.filter(lambda x: x["ASIN"] == ASIN).first()
    #obter uma lista de similares
    similars = product["similar"].split("  ")[1:]
    print(similars)
    similars_more_sales = prod_ASIN_salesrank.filter(lambda x: x[0] in similars and x[1]<int(product["salesrank"])).collect()
    print(similars_more_sales)


def avg(a,b):
    '''
    funcao usada pelo reduce para calcular a media de avaliacao ao longo do tempo
    '''
    soma = a[1]+b[1]
    avg = float(soma)/float(b[2]+1)
    #mostra a data e a media para esse dia
    print('({},{:.2f})'.format(b[0],avg))
    return (b[0],soma,b[2])



def average_ratings(ASIN):
    '''
    Funcao que dado um produto, mostrar a evolucao diaria das medias de avaliacao ao longo do intervalo de tempo
    '''
    product = products.filter(lambda x: x["ASIN"] == ASIN).first()
    enum = list(enumerate(product["reviews"]))
    reviews = sc.parallelize(enum)
    reviews_tuple = reviews.map(lambda x: (x[1]["date"],x[1]["rating"],x[0]))
    reviews_tuple.reduce(avg)

def sales_leader_1_group(group):
    '''
    Funcao que lista os 10 produtos lideres de venda para um grupo de produtos
    '''
    #map => (group,(salesrank,ASIN))
    products_tuples = products.filter(lambda x: "group" in x and x["group"]==group).map(lambda x: (x["group"],(int(x["salesrank"]),x["ASIN"])))
    leaders_10 = products_tuples.sortBy(lambda x: x[1][0]).take(10)
    print(leaders_10)
    

def sales_leader_10_all_groups():
    sales_leader_1_group("Book")
    sales_leader_1_group("Music")
    sales_leader_1_group("DVD")
    sales_leader_1_group("Video")

def parsing1(x):
    asin = x["ASIN"]
    reviews = x["reviews"]
    helpful = []
    for r in reviews:
        helpful.append((asin,int(r["helpful"])))
    return helpful

def avg_helpful():
    '''
    Funcao que lista os 10 produtos com a maior media de avaliacoes uteis positivas por produto
    '''
    products_tuple = products.filter(lambda x: "reviews" in x and isinstance(x["reviews"],list))\
                             .flatMap(parsing1)
    total_by_helpful = products_tuple \
        .mapValues(lambda x: (x,1)) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    avg_by_helpful = total_by_helpful.mapValues(lambda x: float(x[0]) / float(x[1])) \
                                     .sortBy(lambda x: x[1],False) \
                                     .take(10)
    print(avg_by_helpful)

def parsing2(x):
    helpful = []
    for r in x["reviews"]:
        helpful.append(int(r["helpful"]))
    categories = []
    for c in x["categories"]:
        cat = c.split("|")[-1]
        categories.append(cat)
    
    output = []
    for c in categories:
        for h in helpful:
            output.append((c,h))
    
    return output



def avg_categories():
    '''
    Funcao que lista as 5 categorias de produto com a maior media de avaliacoes uteis positivas por produto
    '''
    products_tuple = products.filter(lambda x: "reviews" in x and isinstance(x["reviews"],list))\
                             .flatMap(parsing2)

    total_by_cat = products_tuple \
        .mapValues(lambda x: (x,1)) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    avg_by_cat = total_by_cat.mapValues(lambda x: float(x[0]) / float(x[1])) \
                                     .sortBy(lambda x: x[1],False) \
                                     .take(10)
    print(avg_by_cat)

def parsing3(x):    
    customers = []
    for r in x["reviews"]:
        customers.append((r["customer_id"],1))

    return customers

def top_10_customers_reviewers(group):
    '''
    Funcao que lista os 10 clientes que mais fizeram comentarios por grupo de produto
    '''
    customer_tuple = products.filter(lambda x: "group" in x and x["group"]==group)\
                             .filter(lambda x: "reviews" in x and isinstance(x["reviews"],list))\
                             .flatMap(parsing3)
    customer_10 = customer_tuple.reduceByKey(lambda x,y: x+y)\
                                .sortBy(lambda x: x[1],False)\
                                .map(lambda x: (group,x[0],x[1]))\
                                .take(10)
    for c in customer_10:
        print(c)                            
    
def customer_reviewers_by_group():
    top_10_customers_reviewers("Book")
    top_10_customers_reviewers("Music")
    top_10_customers_reviewers("DVD")
    top_10_customers_reviewers("Video")

#reviews('0738700797')
#similar('B000007R0T')
#average_ratings('0738700797')
#sales_leader_10_all_groups()
#avg_helpful()
#avg_categories()
#customer_reviewers_by_group()