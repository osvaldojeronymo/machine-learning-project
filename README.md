# MBD-mini · EDA por objetivos (targets & client_split)

Este pacote entrega a EDA segmentada em 4 objetivos:
1) **Esquema & Volumetria**  
2) **Prevalência** por mês/fold  
3) **Baseline AUPRC ≈ prevalência**  
4) **Artefatos & Manifest**

## Estrutura
```
notebooks/
  01_explore_mbd_mini.ipynb
src/eda/
  io_tar.py            # leitura TAR + Parquet particionado (reconstrói fold)
  check_schema_vol.py  # esquema, vazamento, volumetria
  prevalence.py        # tabela de prevalência
  baseline.py          # baseline AUPRC a partir da prevalência
  artifacts.py         # manifest.json (hashes, caminhos, notas)
reports/               # saída (json/csv) será gerada aqui
```

## Requisitos
- Python 3.10+
- pandas
- pyarrow (ou fastparquet)

```
pip install -r requirements.txt
```

## Como usar
1. Abra `notebooks/01_explore_mbd_mini.ipynb` no Jupyter/VSCode.  
2. Ajuste `ROOT`, `PATH_TARGETS`, `PATH_SPLIT` na célula **Setup & Config**.  
3. Rode as células em ordem. Artefatos serão salvos em `./reports`.

> Observação: os arquivos no dataset estão particionados como `.../fold=K/part-*.parquet`.  
> O módulo `io_tar.py` **reconstrói `fold`** como coluna a partir do caminho e normaliza `mon` para `Period[M]`.
