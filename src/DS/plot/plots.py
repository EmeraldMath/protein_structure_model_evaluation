from mySecret import db_host, db, db_user, db_password, AWS_ACCESS_KEY, AWS_SECRET_KEY
import pandas as pd
import psycopg2
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy

def main():
    
    conn = psycopg2.connect(host=db_host, database=db,user=db_user,password=db_password)
    query = "select candidate_id, score from casp8"
    df = pd.read_sql_query(query, con = conn)
    df['score'] = df['score'].apply(numpy.round)
    fre = df.groupby('score')['candidate_id'].size()
    df_count = pd.DataFrame({'score': fre.index, 'count':fre.values})
    ax = df_count.reset_index().plot(kind = 'bar', x='score', y='count')
    ax.set_xlabel("score")
    ax.set_ylabel("count")
    ax.set_title("frequency distribution of scores in CASP8")
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=4)
    ax.figure.savefig('score_casp8.pdf')

if __name__ == "__main__":
    main()