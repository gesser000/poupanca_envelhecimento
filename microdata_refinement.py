import pandas as pd

class MDR:

    def __init__(self) -> None:
        
        self.YEARS = [
            "1991",
            "2000",
            "2010"
        ]

        self.DOM_COLUMNS = [
            "ano",
            "UF",
            "codmun",
            "sit_setor_C",
            "especie",
            "cond_ocup_B",
            "ilum_eletr",
            "lavaroupa",
            "n_pes_dom",
            "sanitario",
            "renda_dom",
            "deflator",
            "conversor",
            "peso_dom"
        ]
        
        self.PES_COLUMNS = [
            "ano",
            "UF",
            "codmun",
            "sexo",
            "idade",
            "raca",
            "nasceu_mun",
            "anos_mor_mun",
            "anos_estudoC",
            "filhos_tot",
            "filhos_nasc_vivos",
            "t_mor_mun_80",
            "vive_conjuge",
            "estado_conj",
            "peso_pess"
        ]

    def load_microdata(self, year: str, file_type: str, columns: list) -> pd.DataFrame:
        """Carrega os dados de um determinado ano e tipo"""
        file_path = f"microdados/{year}/CENSO{str(year)[-2:]}_BR_{file_type}_comp.dta"
        try:
            return pd.read_stata(file_path, columns=columns)
        except FileNotFoundError:
            raise FileNotFoundError(f"File not Found: {file_path}")

    def microdata_shrink(self) -> None:

        microdata = []

        for year in self.YEARS:

            pes = self.load_microdata(year, "pes", self.PES_COLUMNS)
            dom = self.load_microdata(year, "dom", self.DOM_COLUMNS)

            pes["tx_65_anos"] = (pes["idade"] >= 65).astype(int)
            pes["tx_14_anos"] = (pes["idade"] <= 14).astype(int)
            pes["tx_pop_ativa"] = ((pes["idade"] >= 14) & (pes["idade"] <= 65)).astype(int)
            pes["tx_casado"] = (pes["estado_conj"].isin([1, 2, 3, 4])).astype(int)
            pes["branco"] = (pes["raca"] == 1).astype(int)
            dom["ocup_propria"] = (dom["cond_ocup_B"] == 1).astype(int)

            pes_vars = ["sexo", "idade", "raca", "branco", "anos_mor_mun", "nasceu_mun", "anos_estudoC", "filhos_tot", "filhos_nasc_vivos", "vive_conjuge", "estado_conj", "tx_65_anos", "tx_14_anos", "tx_pop_ativa", "tx_casado"]
            dom_vars = ["sit_setor_C", "cond_ocup_B", "ocup_propria", "especie", "ilum_eletr", "lavaroupa", "n_pes_dom", "sanitario"]

            # Agrupamento para tabela de pessoas
            pes_mun = pes.groupby(["ano", "UF", "codmun"], as_index=False).agg(
            pop_expandida=("peso_pess", "sum"),
            individuos_amostrados=("peso_pess", "count"),
            **{var: (var, lambda g: (g * pes.loc[g.index, "peso_pess"]).sum() / pes.loc[g.index, "peso_pess"].sum()) for var in pes_vars}
            )

            # Agrupamento para tabela de domicílios
            dom_mun = dom.groupby(["ano", "UF", "codmun"], as_index=False).agg(
            renda_dom=("renda_dom", "sum"),
            deflator=("deflator", "first"),
            conversor=("conversor", "first"),
            **{var: (var, lambda g: (g * dom.loc[g.index, "peso_dom"]).sum() / dom.loc[g.index, "peso_dom"].sum()) for var in dom_vars}
            )

            censo = pd.merge(pes_mun, dom_mun, on=["ano", "UF", "codmun"])

            assert len(pes_mun) == len(dom_mun) == len(censo), f"Error: Difference in the number of municipalities in the year {year}"

            microdata.append(censo)

        microdata_full = pd.concat(microdata)
        microdata_full.to_parquet("microdados/refined/microdata_refined.gzip", compression="gzip")

if __name__ == "__main__":
    microdata_refinement = MDR()
    microdata_refinement.microdata_shrink()