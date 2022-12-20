import pandas as pd
import pymysql
from sqlalchemy import create_engine
from numpy import nan


# 데이터베이스 테이블설계서 목록 관련

def get_table_info(table=None, prep=None, numeric=False, item=None, new=False):
    all_list = raw.select(table='fn_tables')

    if table:
        cond = (all_list.table == table)

        if prep:
            cond = cond & (all_list[prep] == 1)

            if new:
                cond = cond & (all_list.new == 1)

            return all_list[cond].item.to_list()

        elif numeric:
            cond = cond & (all_list.numeric == 1)

            if new:
                cond = cond & (all_list.new == 1)

            return all_list[cond].item.to_list()

        else:
            if new:
                cond = (cond & all_list.new == 1)

            item_name = all_list[cond].T.loc['item'].to_list()
            item_kind = all_list[cond].T.loc['item_kind'].to_list()
            item_code = all_list[cond].T.loc["item_code"].to_list()
            item_name_k = all_list[cond].T.loc["item_nm"].to_list()
            item_freq = all_list[cond].T.loc['item_freq'].to_list()

            return item_name, item_kind, item_code, item_name_k, item_freq

    elif isinstance(item, str):
        item_name, item_kind, item_code, item_name_k, item_freq = (
            all_list[all_list.item == item].iloc[0])[
            ['item', 'item_kind', 'item_code', 'item_nm', 'item_freq']].to_list()

        return item_name, item_kind, item_code, item_name_k, item_freq

    elif isinstance(item, list):
        item_name = all_list[all_list.item.isin(item)].item.to_list()
        item_kind = all_list[all_list.item.isin(item)].item_kind.to_list()
        item_code = all_list[all_list.item.isin(item)].item_code.to_list()
        item_name_k = all_list[all_list.item.isin(item)].item_nm.to_list()
        item_freq = all_list[all_list.item.isin(item)].item_freq.to_list()

        return item_name, item_kind, item_code, item_name_k, item_freq

    elif new:
        return all_list[all_list.new == 1]

    else:
        return all_list


# 데이터베이스 In / Out 관련

class MySQL:

    def __init__(self, db):
        self.con = pymysql.connect(
            user='root',
            passwd='karekano85!',
            host='localhost',
            db=db,
            charset='utf8'
        )
        self.eng = create_engine(
            f'mysql+pymysql://root:karekano85!@localhost:3306/{db}?charset=utf8',
            encoding='utf-8'
        )
        self.cur = self.con.cursor()

    def select(self, sql=None, table=None, date_col=None):
        if table:
            data = pd.read_sql(f'SELECT * FROM {table}', self.eng, parse_dates=date_col)
        else:
            data = pd.read_sql(sql, self.eng, parse_dates=date_col)

        if data.shape[0] == 0:
            return None

        elif data.shape[1] == 1:
            if data.shape[0] == 1:
                return data.iloc[0, 0]
            else:
                return data.iloc[:, 0]
        else:
            return data

    def insert(self, table, df, replace=False):
        if replace:
            df.to_sql(table, self.eng, if_exists='replace', index=False)
        else:
            df.to_sql(table, self.eng, if_exists='append', index=False)

    def upsert(self, table, df):
        """
        table : db 테이블명 (str)
        df : 삽입 및 갱신할 재료 (DataFrame)
        pk : df의 컬럼 중 prime key인 컬럼명의 리스트 (list)
        """
        table_col = raw.select(f'select col, pk from table_col a where a.table = "{table}"')
        col = table_col.col.to_list()
        col_update = table_col.query('pk != "PRI"').col.to_list()

        sql = f"""
        insert into {table} ({', '.join(col)})
        values ({', '.join(['%s']*len(col))})
        on duplicate key update
        {', '.join([f"{i} = values({i})" for i in col_update])}
        """

        args = df.replace(nan, None).values.tolist()

        self.cur.executemany(sql, args)
        self.con.commit()

    def delete(self, table, where=None):
        if where:
            sql = f"DELETE FROM {table} WHERE {where}"
        else:
            sql = f"TRUNCATE TABLE {table}"
        self.cur.execute(sql)
        self.con.commit()


raw = MySQL('inp_raw')
