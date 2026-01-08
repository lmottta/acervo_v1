#!/usr/bin/env python3
"""
Pipeline de Estruturação de Dados - tb_arquivo_acervo
Processa, valida, transforma e estrutura dados do CSV extraído do Impala.
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
import numpy as np

# ----------------------------
# Configurações
# ----------------------------
INPUT_DIR = Path("data/output")
OUTPUT_DIR = Path("data/processed")
REPORTS_DIR = Path("data/reports")

# Criar diretórios
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Nome do arquivo de entrada (mais recente ou especificado)
DEFAULT_INPUT_FILE = None  # None = pega o mais recente

# ----------------------------
# Classes e Funções
# ----------------------------
class ArquivoAcervoPipeline:
    """Pipeline para processamento e estruturação de dados de arquivo de acervo."""
    
    def __init__(self, input_file: Optional[Path] = None):
        """
        Inicializa o pipeline.
        
        Args:
            input_file: Caminho do arquivo CSV de entrada. Se None, usa o mais recente.
        """
        self.input_file = input_file or self._get_latest_csv()
        self.output_file = OUTPUT_DIR / f"tb_arquivo_acervo_estruturado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.report_file = REPORTS_DIR / f"relatorio_estruturacao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        self.df_raw: Optional[pd.DataFrame] = None
        self.df_processed: Optional[pd.DataFrame] = None
        self.stats: Dict[str, Any] = {}
        
    def _get_latest_csv(self) -> Path:
        """Retorna o arquivo CSV mais recente do diretório de entrada."""
        csv_files = list(INPUT_DIR.glob("tb_arquivo_acervo_*.csv"))
        if not csv_files:
            raise FileNotFoundError(
                f"Nenhum arquivo CSV encontrado em {INPUT_DIR}. "
                "Execute o main.py primeiro para gerar o CSV."
            )
        return max(csv_files, key=lambda p: p.stat().st_mtime)
    
    def load_data(self) -> pd.DataFrame:
        """Carrega dados do CSV de entrada."""
        print(f"📂 Carregando dados de: {self.input_file.name}")
        
        try:
            df = pd.read_csv(
                self.input_file,
                sep=";",
                encoding="utf-8-sig",
                dtype={
                    "id_arquivo_acervo": "Int64",
                    "co_protocolo_acervo": "Int64",
                    "id_categoria": "Int64",
                    "id_situacao": "Int64",
                },
                na_values=["", "NULL", "null", "None", "NaN"],
            )
            
            # Converter coluna de data se existir
            if "dt_emissao_etiqueta" in df.columns:
                df["dt_emissao_etiqueta"] = pd.to_datetime(df["dt_emissao_etiqueta"], errors="coerce")
            
            print(f"✅ Dados carregados: {len(df):,} linhas, {len(df.columns)} colunas")
            self.df_raw = df.copy()
            return df
            
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            raise
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida integridade e consistência dos dados."""
        print("\n🔍 Validando dados...")
        
        initial_rows = len(df)
        
        # 1. Validação de chaves primárias
        duplicates = df["id_arquivo_acervo"].duplicated().sum()
        if duplicates > 0:
            print(f"⚠️  Atenção: {duplicates} IDs duplicados encontrados")
            df = df.drop_duplicates(subset=["id_arquivo_acervo"], keep="first")
            print(f"   Removidas {initial_rows - len(df)} linhas duplicadas")
        
        # 2. Validação de valores negativos em IDs
        id_cols = ["id_arquivo_acervo", "co_protocolo_acervo", "id_categoria", "id_situacao"]
        for col in id_cols:
            negatives = (df[col] < 0).sum()
            if negatives > 0:
                print(f"⚠️  Atenção: {negatives} valores negativos em {col}")
        
        # 3. Validação de tipos booleanos
        if "ic_principal" in df.columns:
            invalid_bool = ~df["ic_principal"].isin([True, False, np.nan])
            if invalid_bool.sum() > 0:
                print(f"⚠️  Atenção: {invalid_bool.sum()} valores inválidos em ic_principal")
                df.loc[invalid_bool, "ic_principal"] = False
        
        # 4. Validação de datas futuras (improváveis)
        if "dt_emissao_etiqueta" in df.columns and df["dt_emissao_etiqueta"].notna().any():
            future_dates = (df["dt_emissao_etiqueta"] > datetime.now()).sum()
            if future_dates > 0:
                print(f"⚠️  Atenção: {future_dates} datas futuras encontradas")
        
        print(f"✅ Validação concluída: {len(df):,} linhas válidas")
        return df
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforma e estrutura os dados."""
        print("\n🔧 Transformando dados...")
        
        df = df.copy()
        
        # 1. Normalização de tipos
        print("   📝 Normalizando tipos de dados...")
        
        # IDs como Int64 (nullable integer)
        id_cols = [
            "id_descritor_informacional",
            "id_descritor_catalografico",
            "id_descritor_tecnico",
            "id_descritoes_informacionais_iconografico",
            "id_descritor_catalografico_iconografico",
            "id_arquivo_acervo_pai",
            "ic_publicado_internet",
        ]
        for col in id_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        
        # Booleanos
        bool_cols = ["ic_principal"]
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].fillna(False).astype(bool)
        
        # 2. Tratamento de valores nulos
        print("   🧹 Tratando valores nulos...")
        
        # Campos numéricos: manter NaN (já são Int64 nullable)
        # Campos booleanos: já tratados acima
        
        # 3. Criação de campos derivados
        print("   ✨ Criando campos derivados...")
        
        # Flag de registro completo (tem todos os IDs principais)
        df["ic_registro_completo"] = (
            df["id_categoria"].notna() &
            df["id_situacao"].notna() &
            df["co_protocolo_acervo"].notna()
        )
        
        # Flag de tem descritores
        df["ic_tem_descritores"] = (
            df["id_descritor_informacional"].notna() |
            df["id_descritor_catalografico"].notna() |
            df["id_descritor_tecnico"].notna()
        )
        
        # Flag de tem descritor iconográfico
        df["ic_tem_iconografico"] = (
            df["id_descritoes_informacionais_iconografico"].notna() |
            df["id_descritor_catalografico_iconografico"].notna()
        )
        
        # Flag de tem arquivo pai (é subarquivo)
        df["ic_tem_arquivo_pai"] = df["id_arquivo_acervo_pai"].notna()
        
        # Ano de emissão da etiqueta (se disponível)
        if "dt_emissao_etiqueta" in df.columns:
            df["nu_ano_emissao_etiqueta"] = df["dt_emissao_etiqueta"].dt.year.astype("Int64")
        
        # 4. Ordenação
        df = df.sort_values(
            by=["id_categoria", "co_protocolo_acervo", "id_arquivo_acervo"],
            na_position="last"
        ).reset_index(drop=True)
        
        print(f"✅ Transformação concluída: {len(df):,} linhas processadas")
        return df
    
    def generate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Gera estatísticas dos dados processados."""
        print("\n📊 Gerando estatísticas...")
        
        stats = {
            "total_registros": len(df),
            "total_colunas": len(df.columns),
            "data_processamento": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "arquivo_origem": self.input_file.name,
        }
        
        # Estatísticas por coluna
        stats["colunas"] = {}
        for col in df.columns:
            stats["colunas"][col] = {
                "tipo": str(df[col].dtype),
                "nao_nulos": int(df[col].notna().sum()),
                "nulos": int(df[col].isna().sum()),
                "pct_nulos": float((df[col].isna().sum() / len(df)) * 100) if len(df) > 0 else 0.0,
            }
            
            # Estatísticas específicas por tipo
            if df[col].dtype in ["int64", "Int64", "float64"]:
                stats["colunas"][col]["min"] = float(df[col].min()) if df[col].notna().any() else None
                stats["colunas"][col]["max"] = float(df[col].max()) if df[col].notna().any() else None
                stats["colunas"][col]["media"] = float(df[col].mean()) if df[col].notna().any() else None
            
            if df[col].dtype == "bool":
                stats["colunas"][col]["true_count"] = int(df[col].sum())
                stats["colunas"][col]["false_count"] = int((~df[col]).sum())
        
        # Estatísticas de flags derivadas
        if "ic_registro_completo" in df.columns:
            stats["registros_completos"] = int(df["ic_registro_completo"].sum())
            stats["pct_completos"] = float((df["ic_registro_completo"].sum() / len(df)) * 100) if len(df) > 0 else 0.0
        
        if "ic_tem_descritores" in df.columns:
            stats["com_descritores"] = int(df["ic_tem_descritores"].sum())
        
        if "ic_publicado_internet" in df.columns:
            stats["publicados_internet"] = int(df["ic_publicado_internet"].notna().sum() and df["ic_publicado_internet"].sum())
        
        # Estatísticas por categoria
        if "id_categoria" in df.columns:
            stats["por_categoria"] = df["id_categoria"].value_counts().to_dict()
        
        # Estatísticas por situação
        if "id_situacao" in df.columns:
            stats["por_situacao"] = df["id_situacao"].value_counts().to_dict()
        
        self.stats = stats
        print("✅ Estatísticas geradas")
        return stats
    
    def save_processed_data(self, df: pd.DataFrame) -> Path:
        """Salva dados processados em CSV."""
        print(f"\n💾 Salvando dados processados: {self.output_file.name}")
        
        df.to_csv(
            self.output_file,
            sep=";",
            index=False,
            encoding="utf-8-sig",
            date_format="%Y-%m-%d",
        )
        
        file_size = self.output_file.stat().st_size / (1024 * 1024)
        print(f"✅ Arquivo salvo: {file_size:.2f} MB")
        
        return self.output_file
    
    def generate_report(self) -> Path:
        """Gera relatório textual com estatísticas."""
        print(f"\n📄 Gerando relatório: {self.report_file.name}")
        
        report_lines = [
            "=" * 80,
            "RELATÓRIO DE ESTRUTURAÇÃO DE DADOS - TB_ARQUIVO_ACERVO",
            "=" * 80,
            f"Data/Hora: {self.stats['data_processamento']}",
            f"Arquivo de Origem: {self.stats['arquivo_origem']}",
            "",
            "=" * 80,
            "RESUMO GERAL",
            "=" * 80,
            f"Total de Registros: {self.stats['total_registros']:,}",
            f"Total de Colunas: {self.stats['total_colunas']}",
            "",
        ]
        
        # Estatísticas de qualidade
        if "registros_completos" in self.stats:
            report_lines.extend([
                "=" * 80,
                "QUALIDADE DOS DADOS",
                "=" * 80,
                f"Registros Completos: {self.stats['registros_completos']:,} ({self.stats['pct_completos']:.2f}%)",
            ])
            if "com_descritores" in self.stats:
                report_lines.append(f"Com Descritores: {self.stats['com_descritores']:,}")
            if "publicados_internet" in self.stats:
                report_lines.append(f"Publicados na Internet: {self.stats['publicados_internet']:,}")
            report_lines.append("")
        
        # Estatísticas por categoria
        if "por_categoria" in self.stats:
            report_lines.extend([
                "=" * 80,
                "DISTRIBUIÇÃO POR CATEGORIA",
                "=" * 80,
            ])
            for categoria, count in sorted(self.stats["por_categoria"].items()):
                pct = (count / self.stats["total_registros"]) * 100
                report_lines.append(f"Categoria {categoria}: {count:,} ({pct:.2f}%)")
            report_lines.append("")
        
        # Estatísticas por situação
        if "por_situacao" in self.stats:
            report_lines.extend([
                "=" * 80,
                "DISTRIBUIÇÃO POR SITUAÇÃO",
                "=" * 80,
            ])
            for situacao, count in sorted(self.stats["por_situacao"].items()):
                pct = (count / self.stats["total_registros"]) * 100
                report_lines.append(f"Situação {situacao}: {count:,} ({pct:.2f}%)")
            report_lines.append("")
        
        # Detalhamento por coluna
        report_lines.extend([
            "=" * 80,
            "DETALHAMENTO POR COLUNA",
            "=" * 80,
        ])
        for col_name, col_stats in self.stats["colunas"].items():
            report_lines.extend([
                f"\nColuna: {col_name}",
                f"  Tipo: {col_stats['tipo']}",
                f"  Não Nulos: {col_stats['nao_nulos']:,} ({100 - col_stats['pct_nulos']:.2f}%)",
                f"  Nulos: {col_stats['nulos']:,} ({col_stats['pct_nulos']:.2f}%)",
            ])
            if "min" in col_stats and col_stats["min"] is not None:
                report_lines.append(f"  Min: {col_stats['min']}, Max: {col_stats['max']}, Média: {col_stats['media']:.2f}")
            if "true_count" in col_stats:
                report_lines.append(f"  True: {col_stats['true_count']:,}, False: {col_stats['false_count']:,}")
        
        report_lines.extend([
            "",
            "=" * 80,
            f"Arquivo Processado: {self.output_file.name}",
            "=" * 80,
        ])
        
        # Salva relatório
        with open(self.report_file, "w", encoding="utf-8") as f:
            f.write("\n".join(report_lines))
        
        print(f"✅ Relatório salvo: {self.report_file.name}")
        return self.report_file
    
    def run(self) -> Dict[str, Any]:
        """Executa o pipeline completo."""
        start_time = datetime.now()
        
        print("=" * 80)
        print("🚀 PIPELINE DE ESTRUTURAÇÃO - TB_ARQUIVO_ACERVO")
        print("=" * 80)
        print(f"⏰ Início: {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
        print()
        
        try:
            # 1. Carregar dados
            df = self.load_data()
            
            # 2. Validar dados
            df = self.validate_data(df)
            
            # 3. Transformar dados
            df = self.transform_data(df)
            self.df_processed = df
            
            # 4. Gerar estatísticas
            self.generate_statistics(df)
            
            # 5. Salvar dados processados
            output_file = self.save_processed_data(df)
            
            # 6. Gerar relatório
            report_file = self.generate_report()
            
            # Resumo final
            end_time = datetime.now()
            duration = end_time - start_time
            
            print()
            print("=" * 80)
            print("📊 RESUMO DA EXECUÇÃO")
            print("=" * 80)
            print(f"✅ Status: SUCESSO")
            print(f"📊 Registros processados: {len(df):,}")
            print(f"📋 Colunas finais: {len(df.columns)}")
            print(f"📄 Arquivo processado: {output_file.name}")
            print(f"📄 Relatório gerado: {report_file.name}")
            print(f"⏱️  Duração: {duration}")
            print(f"⏰ Fim: {end_time.strftime('%d/%m/%Y %H:%M:%S')}")
            print("=" * 80)
            
            return {
                "success": True,
                "output_file": output_file,
                "report_file": report_file,
                "stats": self.stats,
                "duration": str(duration),
            }
            
        except Exception as e:
            print()
            print("=" * 80)
            print("❌ ERRO NA EXECUÇÃO")
            print("=" * 80)
            print(f"Erro: {e}")
            import traceback
            traceback.print_exc()
            print("=" * 80)
            return {
                "success": False,
                "error": str(e),
            }


def main():
    """Função principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Pipeline de estruturação de dados tb_arquivo_acervo")
    parser.add_argument(
        "--input",
        type=str,
        help="Caminho do arquivo CSV de entrada (opcional, usa o mais recente se não especificado)",
    )
    
    args = parser.parse_args()
    
    input_file = Path(args.input) if args.input else None
    
    pipeline = ArquivoAcervoPipeline(input_file=input_file)
    result = pipeline.run()
    
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
