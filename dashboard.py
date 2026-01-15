import streamlit as st
import pandas as pd
import subprocess
import os
import json
import time
from datetime import datetime, date
import requests
from urllib.parse import urlparse, parse_qs
from fpdf import FPDF
import io

import glob
import shutil

# Configuração da Página
st.set_page_config(
    page_title="Dashboard ETL Acervo SPU",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constantes e Caminhos
HISTORY_FILE = "history.json"
LOGS_DIR = "logs"
PROCESSED_DIR = r"data/processed"
OLD_DIR = os.path.join(PROCESSED_DIR, "old")
DATA_FILE = os.path.join(PROCESSED_DIR, "tb_arquivo_acervo_estruturado.csv")

# Garantir diretórios
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(OLD_DIR, exist_ok=True)

# --- Funções Auxiliares ---

def get_latest_data_file():
    """Retorna o caminho do arquivo mais recente na pasta processed."""
    try:
        # Padrão de busca para arquivos CSV na pasta processed
        search_pattern = os.path.join(PROCESSED_DIR, "*.csv")
        files = glob.glob(search_pattern)
        
        # Filtra arquivos que estão na pasta old (caso o glob pegue recursivamente, o que não deve acontecer por padrão, mas garante)
        files = [f for f in files if "old" not in os.path.dirname(f)]
        
        if not files:
            return None
            
        # Retorna o arquivo mais recente
        latest_file = max(files, key=os.path.getmtime)
        return latest_file
    except Exception as e:
        st.error(f"Erro ao buscar arquivo mais recente: {e}")
        return None

def archive_old_files():
    """Move arquivos processados antigos para a pasta 'old', mantendo apenas o mais recente na pasta processed."""
    try:
        # Busca todos os CSVs na pasta processed
        search_pattern = os.path.join(PROCESSED_DIR, "*.csv")
        files = glob.glob(search_pattern)
        # Garante que não pega arquivos dentro de subdiretórios (como old)
        files = [f for f in files if os.path.dirname(f) == os.path.abspath(PROCESSED_DIR) or os.path.dirname(f) == PROCESSED_DIR]
        
        if not files or len(files) <= 1:
            return
            
        # Ordena por data de modificação (mais recente primeiro)
        files.sort(key=os.path.getmtime, reverse=True)
        
        # Mantém o primeiro (files[0]) e move o restante (files[1:])
        files_to_move = files[1:]
        
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for file_path in files_to_move:
            filename = os.path.basename(file_path)
            # Adiciona timestamp se não tiver
            if "_" not in filename or not any(char.isdigit() for char in filename):
                name, ext = os.path.splitext(filename)
                new_name = f"{name}_archived_{current_time}{ext}"
            else:
                new_name = filename
                
            dest_path = os.path.join(OLD_DIR, new_name)
            shutil.move(file_path, dest_path)
            
    except Exception as e:
        st.error(f"Erro ao arquivar arquivos antigos: {e}")

def load_history():
    """Carrega o histórico de execuções."""
    if not os.path.exists(HISTORY_FILE):
        return []
    with open(HISTORY_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []

def save_history_entry(entry):
    """Salva uma nova entrada no histórico."""
    history = load_history()
    history.append(entry)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=4, default=str)

def get_file_content_as_bytes(file_path):
    with open(file_path, "rb") as f:
        return f.read()

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def to_pdf(df):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=8)
    
    def safe_text(text):
        try:
            return str(text).encode('latin-1', 'replace').decode('latin-1')
        except:
            return str(text)

    pdf.cell(200, 10, text=safe_text(f"Relatório de Dados - {datetime.now().strftime('%d/%m/%Y')}"), ln=1, align="C")
    
    # Simples dump das primeiras colunas/linhas para não quebrar o PDF
    # PDF real de tabela larga é complexo, faremos um resumo
    text = safe_text(f"Total de Registros: {len(df)}\n\nAmostra dos dados (primeiras 5 linhas):\n\n")
    pdf.multi_cell(0, 5, text)
    
    cols = list(df.columns)[:5] # Pegar apenas 5 primeiras colunas para caber
    pdf.cell(0, 5, safe_text(f"Colunas (Amostra): {', '.join(cols)}"), ln=1)
    pdf.ln()
    
    for i, row in df.head(10).iterrows():
        row_txt = " | ".join([str(row[c])[:20] for c in cols])
        pdf.multi_cell(0, 5, text=safe_text(row_txt))
        pdf.ln()
        
    return bytes(pdf.output())

# --- Layout Principal ---

st.title("🚀 Gerenciador ETL - Acervo Memória SPU")
st.markdown("Painel de controle para execução, visualização e monitoramento do processo ETL.")

tabs = st.tabs(["▶️ Execução do Script", "👁️ Visualização de Dados", "🛠️ Teste de API", "📜 Logs e Histórico"])

# --- TAB 1: Execução ---
with tabs[0]:
    st.header("Execução do Pipeline ETL")
    
    col_exec_1, col_exec_2 = st.columns([1, 3])
    
    with col_exec_1:
        st.info("Configuração da Execução")
        
        # Seleção do Script
        script_option = st.radio(
            "Selecione o Script ETL:",
            ["etl_acervo_new.py (Novo)", "etl_acervo.py (Antigo)"],
            index=0,
            help="Escolha qual versão do script de extração deseja executar."
        )
        
        selected_script = "etl_acervo_new.py" if "Novo" in script_option else "etl_acervo.py"
        
        st.divider()
        
        st.markdown(f"**Script Selecionado:** `{selected_script}`")
        
        btn_run = st.button("Iniciar Execução ETL", type="primary", use_container_width=True)
    
    with col_exec_2:
        status_container = st.container()
        progress_bar = st.progress(0)
        log_expander = st.expander("Logs em Tempo Real", expanded=True)
        log_text = log_expander.empty()
    
    if btn_run:
        # Preparação
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"etl_{timestamp}.log"
        log_path = os.path.join(LOGS_DIR, log_filename)
        
        start_time = datetime.now()
        
        status_container.info(f"⏳ Inicializando processo com {selected_script}...")
        
        # Execução
        cmd = ["python", selected_script]
        
        # Variáveis de ambiente para forçar saída sem buffer
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        
        try:
            with open(log_path, "w", encoding="utf-8") as log_file:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    encoding="utf-8",
                    env=env
                )
                
                full_log = ""
                
                # Leitura em tempo real
                while True:
                    line = process.stdout.readline()
                    if not line and process.poll() is not None:
                        break
                    
                    if line:
                        full_log += line
                        log_file.write(line)
                        log_file.flush() # Garante escrita no disco
                        
                        # Atualiza UI
                        log_text.code(full_log[-2000:], language="bash") # Mostra ultimos 2000 chars
                        
                        # Atualiza Barra de Progresso (Heurística baseada nos prints do script)
                        if "Conectando ao Impala" in line:
                            progress_bar.progress(10, text="Conectando ao Banco de Dados...")
                        elif "Executando query" in line:
                            progress_bar.progress(30, text="Extraindo dados (pode demorar)...")
                        elif "Estruturando dados" in line:
                            progress_bar.progress(60, text="Processando e transformando dados...")
                        elif "Salvando arquivo" in line:
                            progress_bar.progress(80, text="Gerando arquivos de saída...")
                        elif "Fim do ETL" in line:
                            progress_bar.progress(100, text="Finalizado!")

                return_code = process.poll()
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # Registro no Histórico
                status = "Success" if return_code == 0 else "Error"
                
                entry = {
                    "id": timestamp,
                    "date": start_time.isoformat(),
                    "duration_seconds": duration,
                    "status": status,
                    "log_file": log_filename,
                    "script": selected_script
                }
                save_history_entry(entry)
                
                if return_code == 0:
                    status_container.success(f"✅ Execução concluída com sucesso em {duration:.2f}s!")
                    st.toast("ETL finalizado com sucesso!", icon="✅")
                    
                    # Arquiva arquivos antigos mantendo apenas o mais recente
                    with st.spinner("Arquivando versões antigas..."):
                        archive_old_files()
                        st.info("🗄️ Arquivos antigos movidos para pasta 'old'.")
                        
                else:
                    status_container.error("❌ Erro durante a execução. Verifique os logs.")
                    st.toast("Erro na execução do ETL.", icon="❌")
                    
        except Exception as e:
            status_container.error(f"❌ Falha ao iniciar processo: {str(e)}")

# --- TAB 2: Visualização ---
with tabs[1]:
    st.header("Visualização e Exportação")
    
    # Busca dinâmica do arquivo mais recente
    latest_file_viz = get_latest_data_file()
    target_file_viz = latest_file_viz if latest_file_viz else DATA_FILE
    
    if target_file_viz and os.path.exists(target_file_viz):
        st.caption(f"📂 Fonte de Dados: `{os.path.basename(target_file_viz)}`")
        
        if st.checkbox("Carregar Dados para Visualização", value=False):
            # Carregamento Otimizado (Cache)
            @st.cache_data(ttl=60) # Cache por 1 min para permitir reload
            def load_data(path):
                return pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str) # Ler tudo como string p/ segurança visual
            
            try:
                df = load_data(target_file_viz)
                
                # Métricas
                m1, m2, m3 = st.columns(3)
                m1.metric("Total de Linhas", f"{len(df):,}")
                m2.metric("Total de Colunas", len(df.columns))
                file_stats = os.stat(target_file_viz)
                m3.metric("Tamanho do Arquivo", f"{file_stats.st_size / (1024*1024):.2f} MB")
                
                st.divider()
                
                # Paginação e Filtros
                col_pag_1, col_pag_2 = st.columns([1, 3])
                with col_pag_1:
                    rows_per_page = st.selectbox("Linhas por página", [10, 50, 100, 500, 1000], index=1)
                
                total_pages = (len(df) // rows_per_page) + (1 if len(df) % rows_per_page > 0 else 0)
                
                with col_pag_2:
                    page_number = st.number_input("Página", min_value=1, max_value=total_pages, value=1)
                
                start_idx = (page_number - 1) * rows_per_page
                end_idx = start_idx + rows_per_page
                
                st.caption(f"Exibindo linhas {start_idx + 1} a {min(end_idx, len(df))} de {len(df)}")
                
                # Exibição do DataFrame
                st.dataframe(
                    df.iloc[start_idx:end_idx],
                    use_container_width=True,
                    height=400
                )
                
                st.divider()
                
                # Seção de Exportação e Download
                st.subheader("📤 Exportação e Download")
                
                exp_col1, exp_col2 = st.columns(2)
                
                with exp_col1:
                    st.markdown("### 💾 Downloads")
                    
                    # CSV
                    csv_data = df.to_csv(sep=";", index=False).encode('utf-8-sig')
                    st.download_button("Download CSV", data=csv_data, file_name="acervo.csv", mime="text/csv", use_container_width=True)
                    
                    # Excel (XLSX)
                    try:
                        # Necessário instalar xlsxwriter ou openpyxl se não tiver no ambiente, mas openpyxl já temos
                        st.download_button("Download XLSX", data=to_excel(df), file_name="acervo.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                    except Exception as e:
                        st.warning(f"Excel indisponível: {e}")

                    # PDF
                    try:
                        st.download_button("Download PDF (Resumo)", data=to_pdf(df), file_name="acervo_resumo.pdf", mime="application/pdf", use_container_width=True)
                    except Exception as e:
                        st.warning(f"PDF indisponível: {e}")

                    # TXT
                    st.download_button("Download TXT", data=csv_data, file_name="acervo.txt", mime="text/plain", use_container_width=True)

                with exp_col2:
                    st.markdown("### 🌐 Envio via API (POST)")
                    post_url = st.text_input("URL de Destino (Endpoint)", value="https://httpbin.org/post")
                    
                    c_test, c_send = st.columns(2)
                    
                    if c_test.button("Testar Conexão", use_container_width=True):
                        try:
                            # Teste simples (OPTIONS ou HEAD ou GET)
                            r = requests.get(post_url, timeout=5)
                            if r.status_code < 400:
                                st.success(f"Conexão OK! Status: {r.status_code}")
                            else:
                                st.warning(f"Conexão respondeu com Status: {r.status_code}")
                        except Exception as e:
                            st.error(f"Erro ao conectar: {e}")
                    
                    if c_send.button("Enviar Dados (POST)", type="primary", use_container_width=True):
                        try:
                            # Envia uma amostra JSON para não travar
                            payload = df.head(100).to_dict(orient="records")
                            with st.spinner("Enviando amostra (100 registros)..."):
                                r = requests.post(post_url, json=payload, timeout=10)
                                if r.status_code in [200, 201]:
                                    st.success(f"Enviado com sucesso! ID: {r.status_code}")
                                    st.json(r.json())
                                else:
                                    st.error(f"Falha no envio: {r.status_code} - {r.text}")
                        except Exception as e:
                            st.error(f"Erro no envio: {e}")

            except Exception as e:
                st.error(f"Erro ao ler arquivo de dados: {e}")
                st.code(str(e))
        else:
            st.info("Marque a caixa acima para carregar e visualizar os dados.")
    else:
        st.warning("⚠️ Arquivo de dados não encontrado. Execute o ETL primeiro.")

# --- TAB 3: Test_API ---
with tabs[2]:
    st.header("🛠️ Teste de API (Estilo Postman)")

    # Determina o arquivo mais recente dinamicamente (recalculando caso a aba tenha sido acessada diretamente)
    # Isso é necessário porque 'latest_file' foi definido dentro do escopo da TAB 2 anteriormente,
    # mas o Streamlit roda o script inteiro a cada interação.
    # Garantimos que ele esteja disponível aqui.
    if 'latest_file' not in locals():
        latest_file = get_latest_data_file()

    # --- Busca Local de Arquivos ---
    st.subheader("🔍 Consultar Arquivo na Base Local")
    st.caption("Busque pelo ID ou Nome do Arquivo na base extraída para preencher automaticamente os parâmetros da API.")
    
    # Usa o arquivo mais recente identificado anteriormente
    target_file = latest_file if latest_file else DATA_FILE
    
    if target_file and os.path.exists(target_file):
        col_search_1, col_search_2 = st.columns([3, 1])
        with col_search_1:
            search_term = st.text_input("Digite o ID (id_arquivo_acervo) ou Nome do Arquivo:", key="search_term_api")
        
        with col_search_2:
            st.write("") # Spacer
            st.write("") 
            # Botão apenas para forçar refresh se necessário, mas o text_input já aciona
            st.button("🔄 Atualizar", use_container_width=True)

        if search_term:
            @st.cache_data(ttl=60)
            def load_lookup_data(path):
                # Carrega apenas colunas necessárias
                try:
                    return pd.read_csv(path, sep=";", usecols=["id_arquivo_acervo", "nome_arquivo_gravado"], dtype=str)
                except ValueError:
                    # Fallback se colunas não existirem (CSV antigo)
                    return pd.read_csv(path, sep=";", dtype=str)
            
            try:
                df_lookup = load_lookup_data(target_file)
                
                # Verifica se colunas existem
                if "id_arquivo_acervo" in df_lookup.columns and "nome_arquivo_gravado" in df_lookup.columns:
                    
                    # Lógica de Busca Inteligente
                    # Se for apenas números, prioriza busca exata pelo ID para evitar conflito com timestamps nos nomes de arquivo
                    if search_term.isdigit():
                         mask = df_lookup["id_arquivo_acervo"] == search_term
                    else:
                         mask = (
                             df_lookup["id_arquivo_acervo"].str.contains(search_term, case=False, na=False) | 
                             df_lookup["nome_arquivo_gravado"].str.contains(search_term, case=False, na=False)
                         )
                    
                    results = df_lookup[mask].head(10)
                    
                    if not results.empty:
                        st.success(f"Encontrados {len(results)} registros (mostrando top 10):")
                        
                        for idx, row in results.iterrows():
                            # Trata múltiplos arquivos (separados por ||)
                            files_in_row = str(row["nome_arquivo_gravado"]).split("||")
                            
                            for f_name in files_in_row:
                                if not f_name or f_name.lower() == 'nan': continue
                                f_name_clean = f_name.strip()
                                
                                c_res1, c_res2 = st.columns([4, 1])
                                with c_res1:
                                    st.markdown(f"**ID:** `{row['id_arquivo_acervo']}` | **Arquivo:** `{f_name_clean}`")
                                with c_res2:
                                    if st.button("Usar", key=f"btn_{row['id_arquivo_acervo']}_{f_name_clean}"):
                                        st.session_state['dyn_file'] = f_name_clean
                                        # Atualiza URL imediatamente
                                        bucket = st.session_state.get('dyn_bucket', 'arquivos-acervo')
                                        base_url = "https://spuservices.spu.gestao.gov.br/acervo/arquivo"
                                        st.session_state['api_url'] = f"{base_url}/{bucket}/{f_name_clean}"
                                        st.rerun()
                                st.divider()
                    else:
                        st.warning("Nenhum resultado encontrado.")
                else:
                    st.error("Colunas 'id_arquivo_acervo' ou 'nome_arquivo_gravado' não encontradas no CSV.")
            except Exception as e:
                st.error(f"Erro ao buscar dados: {e}")
    else:
        st.info("⚠️ Execute o ETL (Aba 1) para gerar a base de dados para consulta.")

    st.divider()

    st.markdown("Execute requisições HTTP diretamente daqui. Importe comandos cURL para preenchimento rápido.")

    # --- Importador de cURL ---
    with st.expander("📥 Importar comando cURL", expanded=False):
        curl_input = st.text_area("Cole seu comando cURL aqui:", height=100)
        if st.button("Importar cURL"):
            if curl_input:
                try:
                    # Parseamento básico de cURL
                    import shlex
                    
                    # Remove quebras de linha e backslashes para unificar o comando
                    clean_curl = curl_input.replace("\\\n", " ").replace("\\", "")
                    tokens = shlex.split(clean_curl)
                    
                    parsed_method = "GET" # Default
                    parsed_url = ""
                    parsed_headers = {}
                    parsed_body = ""
                    
                    i = 0
                    while i < len(tokens):
                        token = tokens[i]
                        
                        if token == "curl":
                            pass
                        elif token in ["-X", "--request"]:
                            parsed_method = tokens[i+1]
                            i += 1
                        elif token in ["-H", "--header"]:
                            header_parts = tokens[i+1].split(":", 1)
                            if len(header_parts) == 2:
                                parsed_headers[header_parts[0].strip()] = header_parts[1].strip()
                            i += 1
                        elif token in ["-d", "--data", "--data-raw"]:
                            parsed_body = tokens[i+1]
                            # Se body existe e método era GET, muda para POST automaticamente (comportamento padrão curl)
                            if parsed_method == "GET": 
                                parsed_method = "POST"
                            i += 1
                        elif token.startswith("http"):
                            parsed = urlparse(token)
                            parsed_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                            
                            # Extrair query params
                            q_params = parse_qs(parsed.query)
                            flat_params = []
                            for k, v_list in q_params.items():
                                for v in v_list:
                                    flat_params.append({"Key": k, "Value": v})
                            
                            st.session_state['api_params_data'] = flat_params
                        elif token == "--output": # Ignora output file
                            i += 1
                        
                        i += 1
                    
                    # Salva no Session State para atualizar os widgets
                    st.session_state['api_method'] = parsed_method
                    st.session_state['api_url'] = parsed_url
                    st.session_state['api_headers'] = json.dumps(parsed_headers, indent=2)
                    st.session_state['api_body'] = parsed_body
                    
                    st.success("cURL importado com sucesso!")
                    
                except Exception as e:
                    st.error(f"Erro ao importar cURL: {e}")

    st.divider()

    # --- Construção Dinâmica de URL ---
    st.subheader("🔧 Parâmetros Dinâmicos")
    st.caption("Edite os campos abaixo para atualizar a URL da requisição automaticamente.")

    # Callback para atualizar a URL principal
    def update_api_url():
        bucket = st.session_state.get('dyn_bucket', 'arquivos-acervo')
        arquivo = st.session_state.get('dyn_file', '')
        base_url = "https://spuservices.spu.gestao.gov.br/acervo/arquivo"
        
        # Constrói a URL
        if arquivo:
            # Remove barras extras se o usuário digitar
            bucket = bucket.strip("/")
            arquivo = arquivo.strip("/")
            new_url = f"{base_url}/{bucket}/{arquivo}"
            st.session_state['api_url'] = new_url

    col_dyn_1, col_dyn_2 = st.columns(2)
    with col_dyn_1:
        st.text_input("Bucket", value="arquivos-acervo", key="dyn_bucket", on_change=update_api_url)
    with col_dyn_2:
        st.text_input("Nome do Arquivo / ID", value="1550846346565-null-8.tif-paraPDF.pdf", key="dyn_file", on_change=update_api_url)

    st.divider()

    # --- Interface de Requisição ---
    col_req_1, col_req_2 = st.columns([1, 4])
    
    # Inicializa URL no Session State se não existir
    if 'api_url' not in st.session_state:
        st.session_state['api_url'] = "https://spuservices.spu.gestao.gov.br/acervo/arquivo/arquivos-acervo/1550846346565-null-8.tif-paraPDF.pdf"

    with col_req_1:
        # Recupera do session_state se existir, senão usa default
        default_method = st.session_state.get('api_method', 'GET')
        # Adiciona key para persistência e atualizações via cURL
        method = st.selectbox("Método", ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"], 
                              index=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"].index(default_method),
                              key="api_method")
    
    with col_req_2:
        # Usa key='api_url' para binding bidirecional com o state (atualizado pelo callback acima)
        url = st.text_input("Request URL", key="api_url")

    # Abas para Headers e Body
    req_tabs = st.tabs(["Headers", "Body", "Params (Query)"])
    
    with req_tabs[0]:
        default_headers = st.session_state.get('api_headers', '{\n  "X-API-Key": "98a7b6c5-d4e3-f2a1-b098-7c6d5e4f3a2b"\n}')
        headers_str = st.text_area("Headers (JSON)", value=default_headers, height=150)
        try:
            headers = json.loads(headers_str)
        except:
            st.error("Formato de JSON inválido nos Headers")
            headers = {}

    with req_tabs[1]:
        default_body = st.session_state.get('api_body', '')
        body_str = st.text_area("Body (Raw/JSON)", value=default_body, height=150)

    with req_tabs[2]:
        st.markdown("Adicione parâmetros de consulta (Query Params) aqui:")
        
        # Inicializa estado se necessário
        if 'api_params_data' not in st.session_state:
            st.session_state['api_params_data'] = []

        # Prepara DataFrame inicial
        df_p = pd.DataFrame(st.session_state['api_params_data'])
        if "Key" not in df_p.columns:
            df_p = pd.DataFrame(columns=["Key", "Value"])
            
        # Editor de Dados
        edited_params_df = st.data_editor(
            df_p, 
            num_rows="dynamic", 
            use_container_width=True,
            column_config={
                "Key": st.column_config.TextColumn("Chave", required=True),
                "Value": st.column_config.TextColumn("Valor", required=True)
            },
            hide_index=True,
            key="params_editor_widget"
        )

    # Botão de Envio
    if st.button("🚀 Send Request", type="primary", use_container_width=True):
        if not url:
            st.warning("Por favor, insira uma URL.")
        else:
            try:
                start_time = time.time()
                
                # Processa Params do Editor
                final_params = {}
                if not edited_params_df.empty:
                    for _, row in edited_params_df.iterrows():
                        k = row["Key"]
                        v = row["Value"]
                        if k:
                            if k in final_params:
                                if isinstance(final_params[k], list):
                                    final_params[k].append(v)
                                else:
                                    final_params[k] = [final_params[k], v]
                            else:
                                final_params[k] = v
                
                # Preparação da requisição
                req_kwargs = {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "params": final_params,
                    "timeout": 30
                }
                
                # Adiciona body se não for GET/HEAD e tiver conteúdo
                if method not in ["GET", "HEAD"] and body_str:
                    # Tenta enviar como JSON se os headers indicarem, senão string bruta
                    if "application/json" in headers.get("Content-Type", ""):
                        try:
                            req_kwargs["json"] = json.loads(body_str)
                        except:
                            req_kwargs["data"] = body_str # Fallback para data
                    else:
                        req_kwargs["data"] = body_str.encode('utf-8')

                with st.spinner("Enviando requisição..."):
                    response = requests.request(**req_kwargs)
                
                duration = (time.time() - start_time) * 1000 # ms
                
                # --- Exibição da Resposta ---
                st.markdown("### Response")
                
                # Status Bar
                status_color = "green" if 200 <= response.status_code < 300 else "orange" if 400 <= response.status_code < 500 else "red"
                st.markdown(f"""
                    <div style="padding: 10px; border-radius: 5px; background-color: #f0f2f6; border-left: 5px solid {status_color};">
                        <span style="font-weight: bold; font-size: 1.2em;">{response.status_code} {response.reason}</span>
                        <span style="float: right; color: gray;">⏱️ {duration:.0f} ms | 📦 {len(response.content) / 1024:.2f} KB</span>
                    </div>
                """, unsafe_allow_html=True)
                
                res_tabs = st.tabs(["Body (Pretty)", "Body (Raw)", "Headers"])
                
                with res_tabs[0]:
                    try:
                        st.json(response.json())
                    except:
                        # Se não for JSON, tenta mostrar texto ou aviso de binário
                        content_type = response.headers.get("Content-Type", "")
                        if "image" in content_type:
                            st.image(response.content)
                        elif "pdf" in content_type:
                            import base64
                            st.success("📄 PDF detectado! Visualização abaixo:")
                            # Codifica o PDF para base64 para incorporar no iframe
                            base64_pdf = base64.b64encode(response.content).decode('utf-8')
                            # Cria um iframe com o PDF embutido
                            pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="800" type="application/pdf"></iframe>'
                            st.markdown(pdf_display, unsafe_allow_html=True)
                            
                            st.download_button("Download PDF", data=response.content, file_name="documento.pdf", mime="application/pdf")
                            
                        elif "octet-stream" in content_type:
                            st.info("⚠️ Conteúdo binário genérico detectado. Faça o download abaixo.")
                            st.download_button("Download Response", data=response.content, file_name="response.bin")
                        else:
                            st.code(response.text)
                
                with res_tabs[1]:
                    st.text_area("Raw Response", value=response.text, height=300)
                    
                with res_tabs[2]:
                    st.json(dict(response.headers))
                    
            except Exception as e:
                st.error(f"Erro na requisição: {str(e)}")

# --- TAB 4: Histórico ---
with tabs[3]:
    st.header("Histórico de Execuções e Logs")
    
    history = load_history()
    
    if not history:
        st.info("Nenhum histórico encontrado.")
    else:
        # Converter para DF para facilitar filtro
        df_hist = pd.DataFrame(history)
        df_hist['date_dt'] = pd.to_datetime(df_hist['date'])
        
        # Filtros
        col_filter_1, col_filter_2 = st.columns(2)
        with col_filter_1:
            date_range = st.date_input(
                "Filtrar por Período",
                value=(df_hist['date_dt'].min().date(), df_hist['date_dt'].max().date()),
                key="hist_date_range"
            )
        
        # Aplicar filtro
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
            mask = (df_hist['date_dt'].dt.date >= start_date) & (df_hist['date_dt'].dt.date <= end_date)
            df_hist_filtered = df_hist.loc[mask]
        else:
            df_hist_filtered = df_hist
            
        # Exibir Tabela
        st.dataframe(
            df_hist_filtered[['id', 'date', 'status', 'duration_seconds', 'log_file']].sort_values('date', ascending=False),
            use_container_width=True,
            column_config={
                "date": st.column_config.DatetimeColumn("Data", format="DD/MM/YYYY HH:mm:ss"),
                "status": st.column_config.TextColumn("Status"),
                "duration_seconds": st.column_config.NumberColumn("Duração (s)", format="%.2f"),
            }
        )
        
        st.divider()
        
        # Visualizador de Log Específico
        st.subheader("🔍 Visualizar Log Detalhado")
        selected_run_id = st.selectbox("Selecione uma execução para ver o log:", df_hist_filtered['id'].unique())
        
        if selected_run_id:
            run_info = df_hist[df_hist['id'] == selected_run_id].iloc[0]
            log_filename = run_info['log_file']
            log_full_path = os.path.join(LOGS_DIR, log_filename)
            
            if os.path.exists(log_full_path):
                with open(log_full_path, "r", encoding="utf-8") as f:
                    st.code(f.read(), language="bash")
            else:
                st.warning(f"Arquivo de log não encontrado: {log_filename}")

