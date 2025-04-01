import pandas as pd
import numpy as np

estban = pd.read_parquet("estban/refined/estban_refined.gzip")
censo = pd.read_parquet("microdados/refined/microdata_refined.gzip")
dicionario = pd.read_csv("dicionarios/dicionario_geografia.csv", encoding = "Latin1", sep=";", decimal=",")

censo.rename(columns={"ano": "dt_year", "UF": "uf", "codmun": "codmun_ibge"}, inplace=True)
censo["dt_year"] = censo["dt_year"].astype(float)
censo["codmun_ibge"] = censo["codmun_ibge"].astype(float)
censo["idosos"] = censo["pop_expandida"] * censo["tx_65_anos"]
censo["pop_ativa"] = censo["pop_expandida"] * censo["tx_pop_ativa"]
censo["razao_dependencia_idosos"] = censo["idosos"] / censo["pop_ativa"]

estban = estban[['dt_year', 'CODMUN_IBGE', 'AGEN_PROCESSADAS', 'VERBETE_420_DEPOSITOS_DE_POUPANCA', 'VERBETE_432_DEPOSITOS_A_PRAZO']].copy()
estban.rename(columns={"CODMUN_IBGE": "codmun_ibge", "AGEN_PROCESSADAS": "agen_processadas", "VERBETE_420_DEPOSITOS_DE_POUPANCA": "depositos_poupanca", "VERBETE_432_DEPOSITOS_A_PRAZO": "depositos_prazo"}, inplace=True)
estban["depositos_totais"] = estban["depositos_poupanca"] + estban["depositos_prazo"]
estban["dt_year"] = estban["dt_year"].astype(float)

df_merged = pd.merge(censo, estban, on=["dt_year", "codmun_ibge"], how="left")
df_merged = pd.merge(df_merged, dicionario, on="codmun_ibge", how="left")

columns = ['sexo', 'idade', 'raca', 'branco',
       'anos_mor_mun', 'nasceu_mun', 'anos_estudoC', 'filhos_tot',
       'filhos_nasc_vivos', 'vive_conjuge', 'estado_conj', 'tx_casado', 'tx_65_anos', 'tx_pop_ativa', 'tx_14_anos', 'sit_setor_C', 'razao_dependencia_idosos', 'cond_ocup_B', 'ocup_propria',
       'especie', 'ilum_eletr', 'lavaroupa', 'n_pes_dom', 'sanitario',
       ]

df_reg_imediata = df_merged.groupby(['dt_year', 'id_regiao',
       'nm_regiao', 'id_uf', 'nm_uf',
       'id_regiao_geografica_imediata',
       'nm_regiao_geografica_imediata'], as_index=False).agg(
           pop_expandida_micro=("pop_expandida", "sum"),
           individuos_amostrados=("individuos_amostrados", "sum"),
           agen_processadas=("agen_processadas", "sum"),
           depositos_poupanca=("depositos_poupanca", "sum"),
           depositos_prazo=("depositos_prazo", "sum"),
           depositos_totais=("depositos_totais", "sum"),
           renda_dom=("renda_dom", "sum"),
           area_km2=("area_km2", "sum"),
           deflator=("deflator", "first"),
           conversor=("conversor", "first"),
        **{var: (var, lambda g: (g * df_merged.loc[g.index, "pop_expandida"]).sum() / df_merged.loc[g.index, "pop_expandida"].sum()) for var in columns}
       )

df_reg_imediata["renda_dom_deflacionada"] = df_reg_imediata["renda_dom"] / df_reg_imediata["deflator"] / df_reg_imediata["conversor"]
df_reg_imediata["depositos_poupanca_deflacionada"] = df_reg_imediata["depositos_poupanca"] / df_reg_imediata["deflator"] / df_reg_imediata["conversor"]
df_reg_imediata["depositos_prazo_deflacionada"] = df_reg_imediata["depositos_prazo"] / df_reg_imediata["deflator"] / df_reg_imediata["conversor"]
df_reg_imediata["depositos_totais_deflacionada"] = df_reg_imediata["depositos_totais"] / df_reg_imediata["deflator"] / df_reg_imediata["conversor"]

df_reg_imediata["renda_dom_deflacionada_per_capita"] = df_reg_imediata["renda_dom_deflacionada"] / df_reg_imediata["pop_expandida_micro"]
df_reg_imediata["depositos_poupanca_deflacionada_per_capita"] = df_reg_imediata["depositos_poupanca_deflacionada"] / df_reg_imediata["pop_expandida_micro"]
df_reg_imediata["depositos_prazo_deflacionada_per_capita"] = df_reg_imediata["depositos_prazo_deflacionada"] / df_reg_imediata["pop_expandida_micro"]
df_reg_imediata["depositos_totais_deflacionada_per_capita"] = df_reg_imediata["depositos_totais_deflacionada"] / df_reg_imediata["pop_expandida_micro"]

df_reg_imediata["log_renda_dom_deflacionada_per_capita"] = np.log(df_reg_imediata["renda_dom_deflacionada_per_capita"])
df_reg_imediata["log_depositos_poupanca_deflacionada_per_capita"] = np.log(df_reg_imediata["depositos_poupanca_deflacionada_per_capita"] + 1)
df_reg_imediata["log_depositos_prazo_deflacionada_per_capita"] = np.log(df_reg_imediata["depositos_prazo_deflacionada_per_capita"] + 1)
df_reg_imediata["log_depositos_totais_deflacionada_per_capita"] = np.log(df_reg_imediata["depositos_totais_deflacionada_per_capita"] + 1)

df_reg_imediata.to_parquet("dados_agrupados/censo_agrupado_por_regioes_imediatas.gzip", compression="gzip")
df_reg_imediata.to_excel("dados_agrupados/censo_agrupado_por_regioes_imediatas.xlsx")