import requests
import json
import psycopg2

# API to get uid of comuni inside our region
api_url = 'https://test.idrogeo.isprambiente.it/api/pir/comuni?cod_reg=08'  # 示例URL，请根据实际API文档调整

# filter Emilia-Romagna
params = {
    'region': 'Emilia-Romagna'
}

# send GET requset
response = requests.get(api_url, params=params)

# check the responding status
if response.status_code == 200:
    data = response.json()
    emilia_romagna_uids = []  # store all the uid of emilia romagna
    for comune in data:
        # print(comune)
        uid = comune['uid']
        emilia_romagna_uids.append(uid)
else:
    print(f"Error: {response.status_code}")

# print(emilia_romagna_uids)

# iterate API url
api_urls = []
for uid in emilia_romagna_uids:
    comuni_url = 'https://test.idrogeo.isprambiente.it/api/pir/comuni/' + str(uid)
    api_urls.append(comuni_url)
print(len(api_url))

# Database connection parameters
db_name = "se4g"
user = "postgres"
password = "551010"
host = "localhost"

conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host)
cursor = conn.cursor()

# 创建新表
create_table_query = """
CREATE TABLE IF NOT EXISTS api_data_new (
    uid INTEGER PRIMARY KEY,
    osmid INTEGER,
    nome VARCHAR(100),
    ar_kmq NUMERIC,
    ar_id_p3 NUMERIC,
    ar_id_p2 NUMERIC,
    ar_id_p1 NUMERIC,
    aridp3_p NUMERIC,
    aridp2_p NUMERIC,
    aridp1_p NUMERIC,
    pop_res011 INTEGER,
    pop_gio INTEGER,
    pop_gio_p NUMERIC,
    pop_adu INTEGER,
    pop_adu_p NUMERIC,
    pop_anz INTEGER,
    pop_anz_p NUMERIC,
    pop_idr_p3 INTEGER,
    pop_idr_p2 INTEGER,
    pop_idr_p1 INTEGER,
    popidp3_p NUMERIC,
    popidp2_p NUMERIC,
    popidp1_p NUMERIC,
    fam_tot INTEGER,
    fam_idr_p3 INTEGER,
    fam_idr_p2 INTEGER,
    fam_idr_p1 INTEGER,
    famidp3_p NUMERIC,
    famidp2_p NUMERIC,
    famidp1_p NUMERIC,
    ed_tot INTEGER,
    ed_idr_p3 INTEGER,
    ed_idr_p2 INTEGER,
    ed_idr_p1 INTEGER,
    edidp3_p NUMERIC,
    edidp2_p NUMERIC,
    edidp1_p NUMERIC,
    im_tot INTEGER,
    im_idr_p3 INTEGER,
    im_idr_p2 INTEGER,
    im_idr_p1 INTEGER,
    imidp3_p NUMERIC,
    imidp2_p NUMERIC,
    imidp1_p NUMERIC,
    n_vir INTEGER,
    bbcc_id_p3 INTEGER,
    bbcc_id_p2 INTEGER,
    bbcc_id_p1 INTEGER,
    bbccidp3_p NUMERIC,
    bbccidp2_p NUMERIC,
    bbccidp1_p NUMERIC,
    ar_fr_p4 NUMERIC,
    ar_fr_p3 NUMERIC,
    ar_fr_p2 NUMERIC,
    ar_fr_p1 NUMERIC,
    ar_fr_aa NUMERIC,
    ar_fr_p3p4 NUMERIC,
    ar_frp4_p NUMERIC,
    ar_frp3_p NUMERIC,
    ar_frp2_p NUMERIC,
    ar_frp1_p NUMERIC,
    ar_fraa_p NUMERIC,
    ar_frp3p4p NUMERIC,
    pop_fr_p4 NUMERIC,
    pop_fr_p3 NUMERIC,
    pop_fr_p2 NUMERIC,
    pop_fr_p1 NUMERIC,
    pop_fr_aa NUMERIC,
    popfr_p3p4 NUMERIC,
    popfrp4_p NUMERIC,
    popfrp3_p NUMERIC,
    popfrp2_p NUMERIC,
    popfrp1_p NUMERIC,
    popfraa_p NUMERIC,
    popfrp3p4p NUMERIC,
    fam_fr_p4 NUMERIC,
    fam_fr_p3 NUMERIC,
    fam_fr_p2 NUMERIC,
    fam_fr_p1 NUMERIC,
    fam_fr_aa NUMERIC,
    famfr_p3p4 NUMERIC,
    famfrp4_p NUMERIC,
    famfrp3_p NUMERIC,
    famfrp2_p NUMERIC,
    famfrp1_p NUMERIC,
    famfraa_p NUMERIC,
    famfrp3p4p NUMERIC,
    ed_fr_p4 NUMERIC,
    ed_fr_p3 NUMERIC,
    ed_fr_p2 NUMERIC,
    ed_fr_p1 NUMERIC,
    ed_fr_aa NUMERIC,
    ed_fr_p3p4 NUMERIC,
    edfrp4_p NUMERIC,
    edfrp3_p NUMERIC,
    edfrp2_p NUMERIC,
    edfrp1_p NUMERIC,
    edfraa_p NUMERIC,
    edfrp3p4p NUMERIC,
    im_fr_p4 NUMERIC,
    im_fr_p3 NUMERIC,
    im_fr_p2 NUMERIC,
    im_fr_p1 NUMERIC,
    im_fr_aa NUMERIC,
    imfr_p3p4 NUMERIC,
    imfrp4_p NUMERIC,
    imfrp3_p NUMERIC,
    imfrp2_p NUMERIC,
    imfrp1_p NUMERIC,
    imfraa_p NUMERIC,
    imfrp3p4p NUMERIC,
    bbcc_fr_p4 NUMERIC,
    bbcc_fr_p3 NUMERIC,
    bbcc_fr_p2 NUMERIC,
    bbcc_fr_p1 NUMERIC,
    bbcc_fr_aa NUMERIC,
    bbccfrp3p4 NUMERIC,
    bbccfrp4_p NUMERIC,
    bbccfrp3_p NUMERIC,
    bbccfrp2_p NUMERIC,
    bbccfrp1_p NUMERIC,
    bbccfraa_p NUMERIC,
    bbccfrp34p NUMERIC,
    cod_rip INTEGER,
    cod_reg INTEGER,
    cod_prov INTEGER,
    pro_com INTEGER
);
"""
cursor.execute(create_table_query)
conn.commit()

# 获取新表中的所有列
cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'api_data_new'")
table_columns = [row[0] for row in cursor.fetchall()]

# 定义所有字段及其默认值
default_fields = {
    "uid": None, "osmid": None, "nome": None, "ar_kmq": None, "ar_id_p3": None, "ar_id_p2": None,
    "ar_id_p1": None, "aridp3_p": None, "aridp2_p": None, "aridp1_p": None, "pop_res011": None,
    "pop_gio": None, "pop_gio_p": None, "pop_adu": None, "pop_adu_p": None, "pop_anz": None,
    "pop_anz_p": None, "pop_idr_p3": None, "pop_idr_p2": None, "pop_idr_p1": None, "popidp3_p": None,
    "popidp2_p": None, "popidp1_p": None, "fam_tot": None, "fam_idr_p3": None, "fam_idr_p2": None,
    "fam_idr_p1": None, "famidp3_p": None, "famidp2_p": None, "famidp1_p": None, "ed_tot": None,
    "ed_idr_p3": None, "ed_idr_p2": None, "ed_idr_p1": None, "edidp3_p": None, "edidp2_p": None,
    "edidp1_p": None, "im_tot": None, "im_idr_p3": None, "im_idr_p2": None, "im_idr_p1": None,
    "imidp3_p": None, "imidp2_p": None, "imidp1_p": None, "n_vir": None, "bbcc_id_p3": None,
    "bbcc_id_p2": None, "bbcc_id_p1": None, "bbccidp3_p": None, "bbccidp2_p": None, "bbccidp1_p": None,
    "ar_fr_p4": None, "ar_fr_p3": None, "ar_fr_p2": None, "ar_fr_p1": None, "ar_fr_aa": None,
    "ar_fr_p3p4": None, "ar_frp4_p": None, "ar_frp3_p": None, "ar_frp2_p": None, "ar_frp1_p": None,
    "ar_fraa_p": None, "ar_frp3p4p": None, "pop_fr_p4": None, "pop_fr_p3": None, "pop_fr_p2": None,
    "pop_fr_p1": None, "pop_fr_aa": None, "popfr_p3p4": None, "popfrp4_p": None, "popfrp3_p": None,
    "popfrp2_p": None, "popfrp1_p": None, "popfraa_p": None, "popfrp3p4p": None, "fam_fr_p4": None,
    "fam_fr_p3": None, "fam_fr_p2": None, "fam_fr_p1": None, "fam_fr_aa": None, "famfr_p3p4": None,
    "famfrp4_p": None, "famfrp3_p": None, "famfrp2_p": None, "famfrp1_p": None, "famfraa_p": None,
    "famfrp3p4p": None, "ed_fr_p4": None, "ed_fr_p3": None, "ed_fr_p2": None, "ed_fr_p1": None,
    "ed_fr_aa": None, "ed_fr_p3p4": None, "edfrp4_p": None, "edfrp3_p": None, "edfrp2_p": None,
    "edfrp1_p": None, "edfraa_p": None, "edfrp3p4p": None, "im_fr_p4": None, "im_fr_p3": None,
    "im_fr_p2": None, "im_fr_p1": None, "im_fr_aa": None, "imfr_p3p4": None, "imfrp4_p": None,
    "imfrp3_p": None, "imfrp2_p": None, "imfrp1_p": None, "imfraa_p": None, "imfrp3p4p": None,
    "bbcc_fr_p4": None, "bbcc_fr_p3": None, "bbcc_fr_p2": None, "bbcc_fr_p1": None, "bbcc_fr_aa": None,
    "bbccfrp3p4": None, "bbccfrp4_p": None, "bbccfrp3_p": None, "bbccfrp2_p": None, "bbccfrp1_p": None,
    "bbccfraa_p": None, "bbccfrp34p": None, "cod_rip": None, "cod_reg": None, "cod_prov": None,
    "pro_com": None
}

# 迭代每个API URL并获取数据
for url in api_urls:
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        # 填充字段值
        fields = default_fields.copy()
        for field in fields:
            if field in data:
                fields[field] = data[field]

        # 过滤掉不存在于数据库表中的字段
        filtered_fields = {k: fields[k] for k in fields if k in table_columns}

        # 插入数据的SQL语句，使用 ON CONFLICT 子句
        insert_query = f"""
            INSERT INTO api_data_new ({', '.join(filtered_fields.keys())})
            VALUES ({', '.join(['%s'] * len(filtered_fields))})
            ON CONFLICT (uid) DO NOTHING
            """

    # 执行插入操作
    cursor.execute(insert_query, list(filtered_fields.values()))
    conn.commit()
else:
    print(f"Failed to fetch data from {url}")

# 关闭连接
cursor.close()
conn.close()