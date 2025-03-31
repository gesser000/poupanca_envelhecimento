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
            "munic",
            "sit_setor_C",
            "especie",
            "ilum_eletr",
            "lavaroupa",
            "n_pes_dom",
            "sanitario",
            "renda_dom",
            "deflator",
            "conversor"
        ]
        
        self.PES_COLUMNS = [
            "ano",
            "UF",
            "munic",
            "sexo",
            "idade",
            "raca",
            "anos_mor_mun",
            "anos_estudoC",
            "filhos_tot",
            "filhos_nasc_vivos",
            "t_mor_mun_80",
            "vive_conjuge",
            "estado_conj"
        ]

    def load_microdata(self, year: str, file_type: str) -> pd.DataFrame:
        """Carrega os dados de um determinado ano e tipo"""
        file_path = f"microdados/{year}/CENSO{str(year)[-2:]}_BR_{file_type}_comp.dta"
        try:
            return pd.read_stata(file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"File not Found: {file_path}")

    def microdata_shrink(self) -> None:

        microdata = []

        for year in self.YEARS:

            pes = self.load_microdata(year, "pes")
            dom = self.load_microdata(year, "dom")

            pes = pes[self.PES_COLUMNS].copy()
            dom = dom[self.DOM_COLUMNS].copy()

            pes_mun = pes.groupby(["ano", "UF", "munic"], as_index=False).mean(numeric_only=True)
            dom_mun = dom.groupby(["ano", "UF", "munic"], as_index=False).mean(numeric_only=True)

            censo = pd.merge(pes_mun, dom_mun, on=["ano", "UF", "munic"])

            assert len(pes_mun) == len(dom_mun) == len(censo), f"Error: Difference in the number of municipalities in the year {year}"

            microdata.append(censo)

        microdata_full = pd.concat(microdata)
        microdata_full.to_parquet("microdados/refined/microdata_refined.gzip", compression="gzip")

if __name__ == "__main__":
    microdata_refinement = MDR()
    microdata_refinement.microdata_shrink()