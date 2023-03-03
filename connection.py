import mysql.connector as mysqldb
import csv
import sys

def read_csv(type):
    path  = f'./datasets/{type}.csv'
    with open(path, 'r',encoding='utf8') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',')
        header = next(reader)
        data = []
        for row in reader:
            iterable = zip(header, row)
            dic = {key : value for key,value in iterable}
            data.append(dic)
            
        return(data)
    
    

def insert_query_movie(dic):
    
    query = 'INSERT INTO movies (id, title, rating, release_date, sort_no, summary, metascore, user_score ) VALUES ({},`{}`,`{}`,`{}`,{},`{}`,{},{})'.format(
                                                                                                                                                        dic['id'],
                                                                                                                                                        dic['title'],
                                                                                                                                                        dic['rating'],
                                                                                                                                                        dic['release_date'],
                                                                                                                                                        dic['sort_no'],
                                                                                                                                                        dic['summary'],
                                                                                                                                                        dic['metascore'],
                                                                                                                                                        dic['user_score']
                                                                                                                                                        )
    return query

def insert_query_game(dic):
    
    query = 'INSERT INTO games (id, metascore, title, platform, user_score, release_date, sort_no, summary) VALUES ({},{},`{}`,`{}`,{},`{}`,{},`{}`)'.format(
                                                                                                                                                        dic['id'],
                                                                                                                                                        dic['metascore'],
                                                                                                                                                        dic['title'],
                                                                                                                                                        dic['platform'],
                                                                                                                                                        dic['user_score'],
                                                                                                                                                        dic['release_date'],
                                                                                                                                                        dic['sort_no'],
                                                                                                                                                        dic['summary']
                                                                                                                                                        )
    return query

                                                                                                                                                        
def execute_insert_games(data):
    for line in data:
        search_id = 'SELECT id FROM games WHERE id={}'.format(line['id'])
        cursor.execute(search_id)
        results = cursor.fetchall()
        if len(results)==0:
            if line['user_score'] == 'tbd':
                line['user_score'] = 0
            line['summary'] = line['summary'].replace('`',"'")
            pre_query = insert_query_game(line).replace('"',"'")
            query = pre_query.replace('`','"')
            cursor.execute(query)
            conexion.commit()
        else:
            continue
        
        
def execute_insert_movies(data):
    for line in data:
        search_id = 'SELECT id FROM movies WHERE id={}'.format(line['id'])
        cursor.execute(search_id)
        results = cursor.fetchall()
        if len(results)==0:
            if line['user_score'] == 'tbd':
                line['user_score'] = 0
            line['summary'] = line['summary'].replace('`',"'")
            pre_query = insert_query_movie(line).replace('"',"'")
            query = pre_query.replace('`','"')
            print(line)
            cursor.execute(query)
            conexion.commit()
        else:
            continue
        
if __name__=='__main__':
    
    conexion = mysqldb.connect(
                                host = 'localhost',
                                user = 'root',
                                password = '141304',
                                database = 'data_metacritics'
                                )
    cursor = conexion.cursor()
    
    type = ['games','movies','animes']
    data_game = read_csv(type[0])
    data_movie = read_csv(type[1])
    
    execute_insert_games(data_game)
    
    execute_insert_movies(data_movie)

    

    
            
    


