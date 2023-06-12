import pandas as pd
obitos = pd.read_parquet("C:/Users/gaolg/OneDrive/Área de Trabalho/IPS Nordeste/bases_tratadas/obitos_tratados_microdados.parquet")

nascidos_vivos = pd.read_parquet("C:/Users/gaolg/OneDrive/Área de Trabalho/IPS Nordeste/bases_tratadas/nascidos_vivos_tratados.parquet")
nascidos_vivos = nascidos_vivos[["cod_municipio", "contagem"]]
nascidos_vivos = nascidos_vivos.rename(columns={"contagem": "nascidos_vivos"})

populacao = pd.read_parquet("C:/Users/gaolg/OneDrive/Área de Trabalho/IPS Nordeste/bases_tratadas/populacao_tratados.parquet")
populacao = populacao[["cod_municipio", "pop_2021"]]
populacao = populacao.rename(columns={"pop_2021": "populacao"})

def filter_group(
        group,
        start_group=0,
        end_group=9,
        habitantes=False,
        n_habitantes=100000,
        n_nascidos_vivos=100000):
    dfs = []
    for n in range(start_group, end_group+1):
        mask = obitos['causa'].str.startswith(f'{group}{n}')
        df = obitos[mask]
        dfs.append(df)
    dfs = pd.concat(dfs)
    dfs = dfs.drop_duplicates(subset="id")
    dfs = dfs[["id_municipio_ocorrencia", "causa"]]

    dfs = dfs.groupby("id_municipio_ocorrencia", as_index=False).count()
    dfs = dfs.sort_values(by="causa", ascending=False)
    dfs = dfs.rename(columns={"id_municipio_ocorrencia":"cod_municipio", "causa":"obitos"})

    if habitantes:
        dfs = dfs.merge(populacao, on=["cod_municipio"], how="left")
        dfs["obitos"] = dfs["obitos"] / dfs["populacao"] * n_habitantes
        dfs = dfs[["cod_municipio", "obitos"]]
    else:
        dfs = dfs.merge(nascidos_vivos, on=["cod_municipio"], how="left")
        dfs["obitos"] = dfs["obitos"] / dfs["nascidos_vivos"] * n_nascidos_vivos
        dfs = dfs[["cod_municipio", "obitos"]] 

    return dfs
def group_by_categories():
    